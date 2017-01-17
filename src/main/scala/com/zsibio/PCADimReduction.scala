package com.zsibio

/**
  * Created by anastasiia on 1/15/17.
  */

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineModel}

class PCADimReduction (sc: SparkContext, sqlContext: SQLContext) extends Serializable {

  def pcaML(ds: DataFrame, k: Int, labels: String = "Region") : DataFrame ={
    val colNames = ds.drop(labels).drop("SampleId").columns
    val assembler = new VectorAssembler()
      .setInputCols(colNames)
      .setOutputCol("features")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(k)

    val pipeline = new Pipeline()
      .setStages(Array(assembler, pca))

    val model = pipeline.fit(ds)
    val pc = model.stages(1).asInstanceOf[PCAModel].pc

    model.transform(ds).select("Region", "SampleId", "pcaFeatures")
  }


}
