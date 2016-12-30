package com.zsibio

/**
  * Created by anastasiia on 12/15/16.
  */

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.feature.{StringIndexer, IndexToString, VectorIndexer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.sysml.api.ml.SVM


class Classification (sc: SparkContext, sqlContext: SQLContext) extends Serializable {

  var _prediction : DataFrame = null
  var _error : Double = Double.NaN
  var _evaluator : MulticlassClassificationEvaluator = null

  private def transformDF(ds: DataFrame, labels: String) : DataFrame ={
    val classVar : RDD[String] = ds.select(labels).rdd.map(x => x.mkString) // Vectors.dense(x.toSeq.toArray.map(x => x.asInstanceOf[Double])))
    val rddX : RDD[Vector] = ds.drop(labels).rdd.map(row => Vectors.dense(row.toSeq.toArray.map(x => x.asInstanceOf[Double]))).cache()
    val transformedDS : DataFrame = sqlContext.createDataFrame(classVar.zip(rddX)).toDF("label", "features")
    transformedDS
  }

  def svmTuning (dsX: DataFrame, labels: String = "Region", nFolds: Int = 10,  cost: Array[Double] = Array(0.01, 0.1, 10), icpt: Array[Int] = Array(0), tol : Array[Double] = Array(0.01), maxIter: Array[Int]= Array(10)) : CrossValidatorModel ={
    val ds = transformDF(dsX, labels)
    //val svmParameters = new SVM("svm", sc, isMultiClass = true).setRegParam(cost).setMaxIter(maxIter)
    // val modelSVM = svmParameters.fit(ds)
    // model.transform(ds).show()
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(ds)

    val svmParameters = new SVM("svm", sc, isMultiClass = true)

    val paramGrid = new ParamGridBuilder().addGrid(svmParameters.regParam, cost).addGrid(svmParameters.icpt, icpt)
      .addGrid(svmParameters.maxOuterIter, maxIter).addGrid(svmParameters.tol, tol).build() // No parameter search

    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    _evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      // "f1", "precision", "recall", "weightedPrecision", "weightedRecall"
      //.setMetricName("scores")

    val pipeline = new Pipeline().setStages(Array(labelIndexer, svmParameters))

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(_evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(nFolds)

    val model = cv.fit(ds)
    model
  }

  def decisionTreesTuning (dsX: DataFrame, labels: String = "Region", nFolds: Int = 10, bins: Array[Int] = Array(10,15), impurity: Array[String] = Array("gini"), depth: Array[Int] = Array(30)) : CrossValidatorModel ={
    val ds = transformDF(dsX, labels)

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(ds)

    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, dt, labelConverter))

    val paramGrid = new ParamGridBuilder()
      .addGrid(dt.maxBins, bins)
      .addGrid(dt.impurity, impurity)
      .addGrid(dt.maxDepth, depth)
      .build()

    _evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(_evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(nFolds)

    val model = cv.fit(ds)
    model
  }

  def randomForestTuning (dsX: DataFrame, labels: String = "Region", nFolds: Int = 10, bins: Array[Int] = Array(10,15), impurity: Array[String] = Array("gini"), depth: Array[Int] = Array(30)) : CrossValidatorModel ={
    val ds = transformDF(dsX, labels)

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(ds)

    val dt = new RandomForestClassifier()
      .setLabelCol("indexedLabel")

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, dt, labelConverter))

    val paramGrid = new ParamGridBuilder()
      .addGrid(dt.maxBins, bins)
      .addGrid(dt.impurity, impurity)
      .addGrid(dt.maxDepth, depth)
      .build()

    _evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(_evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(nFolds)

    val model = cv.fit(ds)
    model
  }

  def predict(model: CrossValidatorModel, dsX: DataFrame, labels: String = "Region") : Unit = {
    val ds = transformDF(dsX, labels)
    _prediction = model.transform(ds).select("label", "features", "prediction")
    _error = 1 - _evaluator.evaluate(_prediction)
  }


}
