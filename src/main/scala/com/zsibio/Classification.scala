package com.zsibio

/**
  * Created by anastasiia on 12/15/16.
  */

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.sysml.api.ml.SVM


class Classification (sc: SparkContext, sqlContext: SQLContext) extends Serializable {

  var _prediction : DataFrame = null
  var _error : Double = Double.NaN
  var _trainingError : Double = Double.NaN
  var _testingError : Double = Double.NaN
  var _evaluator : MulticlassClassificationEvaluator = null

  private def transformDF(ds: DataFrame, labels: String) : DataFrame ={
    val classVar : RDD[String] = ds.select(labels).rdd.map(x => x.mkString) // Vectors.dense(x.toSeq.toArray.map(x => x.asInstanceOf[Double])))
    val rddX : RDD[Vector] = ds.drop(labels).rdd.map(row => Vectors.dense(row.toSeq.toArray.map(x => x.asInstanceOf[Double]))).cache()
    val transformedDS : DataFrame = sqlContext.createDataFrame(classVar.zip(rddX)).toDF("label", "features")
    transformedDS
  }

  def svm(ds: DataFrame, labels: String = "Region", nFolds: Int = 10,  cost: Array[Double] = Array(0.01, 0.1, 10), icpt: Array[Int] = Array(0), tol : Array[Double] = Array(0.01), maxIter: Array[Int]= Array(10)) : CrossValidatorModel ={
    val labelIndexer = new StringIndexer()
      .setInputCol(labels)
      .setOutputCol("label")
      .fit(ds)

    val colNames = ds.drop(labels).columns
    val assembler = new VectorAssembler()
      .setInputCols(colNames)
      .setOutputCol("features")

    val svm = new SVM("svm", sc, isMultiClass = true)

    val paramGrid = new ParamGridBuilder()
      .addGrid(svm.regParam, cost)
      .addGrid(svm.icpt, icpt)
      .addGrid(svm.maxOuterIter, maxIter)
      .addGrid(svm.tol, tol).build() // No parameter search

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    _evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      // "f1", "precision", "recall", "weightedPrecision", "weightedRecall"
      //.setMetricName("scores")

    val pipeline = new Pipeline().setStages(Array(labelIndexer, assembler, svm))//, labelConverter))

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(_evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(nFolds)

    val model = cv.fit(ds)
    model
  }

  def decisionTrees(ds: DataFrame, labels: String = "Region", nFolds: Int = 10, bins: Array[Int] = Array(10, 15, 20), impurity: Array[String] = Array("entropy", "gini"), depth: Array[Int] = Array(4, 6, 8)) : CrossValidatorModel ={
    val labelIndexer = new StringIndexer()
      .setInputCol(labels)
      .setOutputCol("label")
      .fit(ds)

    val colNames = ds.drop(labels).columns
    val assembler = new VectorAssembler()
      .setInputCols(colNames)
      .setOutputCol("features")

    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, assembler, dt, labelConverter))

    val paramGrid = new ParamGridBuilder()
      .addGrid(dt.maxBins, bins)
      .addGrid(dt.maxDepth, depth)
      .addGrid(dt.impurity, impurity)
      .build()

    _evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(_evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(nFolds)

    val model = cv.fit(ds)
    model
  }

  def randomForest (ds: DataFrame, labels: String = "Region", nFolds: Int = 10, bins: Array[Int] = Array(10, 15, 20), impurity: Array[String] = Array("entropy", "gini"), depth: Array[Int] = Array(4, 6, 8)) : CrossValidatorModel ={
    val labelIndexer = new StringIndexer()
      .setInputCol(labels)
      .setOutputCol("label")
      .fit(ds)

    val colNames = ds.drop(labels).columns
    val assembler = new VectorAssembler()
      .setInputCols(colNames)
      .setOutputCol("features")

    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, assembler, rf, labelConverter))

    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.maxBins, bins)
      .addGrid(rf.maxDepth, depth)
      .addGrid(rf.impurity, impurity)
      .build()

    _evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(_evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(nFolds)

    val model = cv.fit(ds)
    model
  }

  def predict(model: CrossValidatorModel, ds: DataFrame, labels: String = "Region") : Unit = {
    _prediction = model.transform(ds).drop("features")
    _error = (1 - _evaluator.evaluate(_prediction)) * 100
  }


}
