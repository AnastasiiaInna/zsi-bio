package com.zsibio

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.mllib.clustering.{BisectingKMeans, BisectingKMeansModel, GaussianMixture, GaussianMixtureModel}

/**
  * Created by anastasiia on 12/13/16.
  */

trait ClusteringMethods {
  def gmm(ds: DataFrame, categoricalVars : Array[String], K : Int, maxIteration: Int = 10): GaussianMixtureModel
  def bkm(ds: DataFrame, categoricalVars : Array[String], K : Int, maxIteration: Int = 10): BisectingKMeansModel
  def predict(model: GaussianMixtureModel, ds: DataFrame, categoricalVars : Array[String]) : DataFrame
  def predict(model: BisectingKMeansModel, ds: DataFrame, categoricalVars : Array[String]) : DataFrame
}

@SerialVersionUID(15L)
class Clustering (sc: SparkContext, sqlContext: SQLContext) extends Serializable with ClusteringMethods{

  private def dfToRDD (ds: DataFrame, categoricalVars : Array[String]) : RDD[Vector] = {
    val arrsnps : Array[DataFrame] = categoricalVars.map{var d : DataFrame = ds; variable => {d = d.drop(variable); d}}
    val snps : DataFrame = arrsnps(arrsnps.length - 1)
    snps.rdd.map(row => Vectors.dense(row.toSeq.toArray.map(x => x.asInstanceOf[Double]))).cache()
  }

  /* Gaussian Mixture */
  def gmm(ds: DataFrame, categoricalVars : Array[String], K : Int, maxIteration: Int = 10): GaussianMixtureModel ={
    val infoSampleRegion = ds.select(categoricalVars.head, categoricalVars.tail: _*)
    val sampleIDrdd : RDD[String] = infoSampleRegion.select("SampleId").rdd.map(row => row.mkString)//toString().substring(1, row.length - 2 ))

    val snpsRDD: RDD[Vector] = dfToRDD(ds, categoricalVars)

    val gmmParameters = new GaussianMixture()
      .setK(K)
      .setMaxIterations(maxIteration)

    val model = gmmParameters.run(snpsRDD)
    model
  }

  /* Bisecting clustering */
  def bkm(ds: DataFrame, categoricalVars : Array[String], K : Int, maxIteration: Int = 10): BisectingKMeansModel = {

    val sampleIDrdd : RDD[String] = ds.select("SampleId").rdd.map(row => row.mkString)//toString().substring(1, row.length - 2 ))
    val snpsRDD: RDD[Vector] = dfToRDD(ds, categoricalVars)

    val bkmParameters = new BisectingKMeans()
      .setK(K)
      .setMaxIterations(maxIteration)

    val model = bkmParameters.run(snpsRDD)
    model
  }

  def predict(model: GaussianMixtureModel, ds: DataFrame, categoricalVars : Array[String]) : DataFrame ={
    val sampleIDrdd : RDD[String] = ds.select("SampleId").rdd.map(row => row.mkString)//toString().substring(1, row.length - 2 ))
    val snpsRDD: RDD[Vector] = dfToRDD(ds, categoricalVars)
    val clusters : RDD[Int] = model.predict(snpsRDD)//.map(cluster => ("Cluster", cluster))//.map(cluster => Vectors.dense(cluster.toDouble))
    val predicted = sampleIDrdd.zip(clusters)//.map{(id, cluster) => (id, cluster)}
    val clustersDF = sqlContext.createDataFrame(predicted).toDF("SampleId", "Predict")
    ds.join(clustersDF, "SampleId")
  }

  def predict(model: BisectingKMeansModel, ds: DataFrame, categoricalVars : Array[String]) : DataFrame = {
    val sampleIDrdd : RDD[String] = ds.select("SampleId").rdd.map(row => row.mkString)//toString().substring(1, row.length - 2 ))
    val snpsRDD: RDD[Vector] = dfToRDD(ds, categoricalVars)
    val clusters: RDD[Int] = model.predict(snpsRDD) //.map(cluster => ("Cluster", cluster))//.map(cluster => Vectors.dense(cluster.toDouble))
    val predicted = sampleIDrdd.zip(clusters) //.map{(id, cluster) => (id, cluster)}
    val clustersDF = sqlContext.createDataFrame(predicted).toDF("SampleId", "Predict")
    ds.join(clustersDF, "SampleId")
  }

  def purity(ds: DataFrame): Double = {
    val sampleCnt = ds.count()
    val labelPredictPair = ds.map(row => (row(0), row(1)))
    val classesCntInCluster = labelPredictPair.map((_, 1L)).reduceByKey(_ + _).map{ case ((k, v), cnt) => (k, (v, cnt))}.groupByKey
    val majorityClassesinCLuster = classesCntInCluster.map{case(_, pairs) => pairs.maxBy(_._2)}
    val correctlyAssignedSUm = majorityClassesinCLuster.values.sum()
    return (correctlyAssignedSUm / sampleCnt)
  }
}

object Clustering extends Serializable {
  def apply (sc: SparkContext, sqlContext: SQLContext): Clustering = new Clustering (sc, sqlContext)
}

