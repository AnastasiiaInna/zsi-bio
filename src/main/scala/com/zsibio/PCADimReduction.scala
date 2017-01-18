package com.zsibio

/**
  * Created by anastasiia on 1/15/17.
  */

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.linalg._
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, svd => brzSvd}
import breeze.linalg.accumulate
import java.util.Arrays

import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}


class PCADimReduction (sc: SparkContext, sqlContext: SQLContext) extends Serializable {

  var _nPC : Int = 0

  def pcaML(ds: DataFrame, nPC: Int, labels: String = "Region", varianceTreshold : Double = 0.7, outputCol : String) : DataFrame ={
    val colNames = ds.drop(labels).drop("SampleId").columns
    val assembler = new VectorAssembler()
      .setInputCols(colNames)
      .setOutputCol("features")
/*
    val standardizer = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setWithMean(true)
      .setWithStd(true)
*/
    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol(outputCol)
      .setK(nPC)

    val pipeline = new Pipeline()
      .setStages(Array(assembler, pca))

    val model = pipeline.fit(ds)
    val pcaFeaturesDS = model.transform(ds).select("Region", "SampleId", outputCol)
    pcaFeaturesDS
    // explainedVariance(pcaFeaturesDS, nPC, varianceTreshold)
  }

  def explainedVariance(pcaResultDS: DataFrame, nPC: Int = 20, varianceTreshold : Double = 0.7, inputCol : String) : DataFrame={
    val pcaFeaturesDS : DataFrame = pcaResultDS.select(inputCol)
    val featuresDense : RDD[DenseVector] = pcaFeaturesDS.rdd.map(row => row(0).asInstanceOf[DenseVector])
    // val featuresVector : RDD[IndexedRow] = featuresDense.map{row => Vectors.dense(row.toArray)}.zipWithIndex().map{case(v, idx) => IndexedRow(idx, v)}
    // val featuresMat : IndexedRowMatrix = new IndexedRowMatrix(featuresVector)
    // val svd: SingularValueDecomposition[IndexedRowMatrix, Matrix] = featuresMat.computeSVD(20, computeU = false)

    val features : RDD[Array[Double]] = featuresDense.map{row => row.toArray}
    val featuresVector : RDD[Vector] = features.map{row => Vectors.dense(row)}
    val featuresMat : RowMatrix = new RowMatrix(featuresVector)
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = featuresMat.computeSVD(nPC, computeU = false)
    val eigenValues = svd.s.toArray

    val variance = eigenValues.map(value => value / eigenValues.sum)
    variance.foreach(println)
    val cumVariance = variance.map{var x : Double = 0; value => x += value; x}
    val numberPC = cumVariance.filter(x => x <= varianceTreshold).size
    _nPC = numberPC

    val featuresOutput = features.take(numberPC).map{row => Vectors.dense(row)}
    var df = sqlContext.createDataFrame(featuresOutput.map(Tuple1.apply)).toDF("pcaFeatures")

    // df = df.withColumn("Region", pcaResultDS("Region"))//.join(pcaResultDS.select("Region", "SampleId"))
    // df = df.withColumn("SampleId", pcaResultDS("SampleId"))

    /*val sampleIDrdd : RDD[String] = pcaResultDS.select("SampleId").rdd.map(row => row.mkString)
    val z = sampleIDrdd.zip(features)
    sqlContext.createDataFrame(z).toDF()
    */

    return df
  }

}
