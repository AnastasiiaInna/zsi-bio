package com.zsibio

import hex.{DataInfo, FrameSplitter}
import hex.kmeans.KMeans
import hex.kmeans.KMeansModel.KMeansParameters
import hex.pca.PCA
import hex.pca.PCAModel.PCAParameters
import hex.pca.PCAModel.PCAParameters.Method
import org.apache.spark.SparkContext
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import water.Key
import water.fvec.Frame

case class OutputParameters(dimentionalityRedMethod: String, clusterMethod: String, trainingFrame: DataFrame, SSW: Double, SSB: Double){
  def this(clusterMethod: String) = this(clusterMethod, null, null, 0, 0)
  def this() = this(null, null, null, 0, 0)
}

trait UnsupervisedMethods {
  protected def getH2ODataFrame (schemaRDD: DataFrame) : Frame
  def splitDataFrame (dataSet: Frame, ratios: Array[Double]): (Frame, Frame)
  def splitDataFrame (dataSet: DataFrame, ratios: Array[Double]): (Frame, Frame)
  def pcaH2O (schemaRDD: DataFrame, ratios: Array[Double], pcaMethod: Method): Frame
  def kMeansH2OTrain(dataSet: Frame, numberClusters: Int): DataFrame
  def writecsv (dataSet: DataFrame, filename: String): Unit
}

@SerialVersionUID(15L)
class Unsupervised[T] (sc: SparkContext, sqlContext: SQLContext) extends Serializable with UnsupervisedMethods{

  // private var _schemaRDD : DataFrame = schemaRDD
  // def schemaRDD_ (value: DataFrame): Unit = _schemaRDD = value

  private var _dimensionalityRedMethod : String = ""
  private var _clusteringMethod : String = ""
  private var _trainingFrame : DataFrame = null
  private var _ssw : Double = 0
  private var _ssb : Double = 0

  val h2oContext = H2OContext.getOrCreate(sc)
  import h2oContext._

  def getH2ODataFrame (schemaRDD: DataFrame) : water.fvec.Frame = {
    val dataFrame = asH2OFrame(schemaRDD)
    dataFrame.replace(dataFrame.find("Region"), dataFrame.vec("Region").toCategoricalVec()).remove()
    dataFrame.update()
    // dataFrame.vecs(Array.range(3, dataFrame.numCols())).map(i => i.toNumericVec)
  }

  def splitDataFrame (dataSet: Frame, ratios: Array[Double]) : (Frame, Frame) ={
    val frameSplitter = new FrameSplitter(dataSet, ratios, Array("training", "validation").map(Key.make[Frame](_)), null)
    water.H2O.submitTask(frameSplitter)
    val splits = frameSplitter.getResult
    (splits(0), splits(1))
  }

  def splitDataFrame (schemaRDD: DataFrame, ratios: Array[Double]) : (Frame, Frame) = {
    val dataSet : Frame = getH2ODataFrame(schemaRDD)
    splitDataFrame(dataSet, ratios)
  }

  def pcaH2O (schemaRDD: DataFrame, ratios: Array[Double] = Array(.7), pcaMethod: Method = Method.GramSVD): Frame = {
    _dimensionalityRedMethod = "PCA_" + pcaMethod.toString
    val dataSet = getH2ODataFrame(schemaRDD)

    var _ratios : Array[Double] = ratios
    if (pcaMethod == Method.GLRM) _ratios = Array(.5)
    val (training, validation) = splitDataFrame(dataSet, _ratios)

    val nFeatures = training.numCols() - 2 // remove SampleId and Region
    val nObservations = training.numRows().toInt

    println(nFeatures, nObservations)

    val pcaParameters = new PCAParameters()
    pcaParameters._train = training._key
    // pcaParameters._valid = validation._key
    pcaParameters._response_column = "Region"
    pcaParameters._ignored_columns = Array("SampleId")
    pcaParameters._k = math.min(nFeatures, nObservations)
    pcaParameters._use_all_factor_levels = true
    pcaParameters._pca_method = pcaMethod
    pcaParameters._max_iterations = 100
    pcaParameters._transform = DataInfo.TransformType.NORMALIZE


    val pcaObject = new PCA(pcaParameters)
    val pcaModel = pcaObject.trainModel.get

    val pcaImportance = pcaModel._output._importance
    val pcaCumVariance = pcaImportance.getCellValues.toList(2).toList
    val pcaEigenvectors = pcaModel._output._eigenvectors

    val totalVariancePerc : Double = .7

    val intPcaCumVariance = pcaCumVariance.map(p => p.get().asInstanceOf[Double])
    val numberPC = intPcaCumVariance.filter(x => x <= totalVariancePerc).size

    val prediction = pcaModel.score(training)
    val pcaDs = prediction

    pcaDs.remove(Array.range(numberPC, pcaEigenvectors.getColDim))
    pcaDs.update()

    pcaDs.add(Array("SampleId", "Region"), Array(training.vec("SampleId").toCategoricalVec(), training.vec("Region").toCategoricalVec()))
    pcaDs.update()

  }

  def kMeansH2OTrain(dataSet: Frame, numberClusters: Int): DataFrame = {
    val kmeansParameters = new KMeansParameters()
    kmeansParameters._train = dataSet._key
    kmeansParameters._response_column = "Region"
    kmeansParameters._ignored_columns = Array("SampleId")
    kmeansParameters._k = numberClusters

    val kmeans = new KMeans(kmeansParameters)
    val kmeansModel = kmeans.trainModel().get()

    val kmeansPrediction = kmeansModel.score(dataSet)

    val predicted = dataSet
    predicted.add("Predict", kmeansPrediction.vec("predict").toCategoricalVec)
    predicted.update()

    _clusteringMethod = s"kmeans_$numberClusters"
    _trainingFrame = asDataFrame(h2oContext.asH2OFrame(predicted))(sqlContext)
    _ssb = kmeansModel._output._betweenss
    _ssw = kmeansModel._output._tot_withinss

    return _trainingFrame
  }

  def writecsv (dataSet: DataFrame, filename: String): Unit ={
    dataSet.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(filename)
  }

  def getOutputParameters : OutputParameters ={
    new OutputParameters(_dimensionalityRedMethod, _clusteringMethod, _trainingFrame, _ssw,  _ssb)
  }

}

object Unsupervised extends Serializable{
  def apply[T](sc: SparkContext, sqlContext: SQLContext): Unsupervised[T] = new Unsupervised[T](sc, sqlContext)
  //def apply[T](sc: SparkContext, schemaRDD: DataFrame): Unsupervised[T] = new Unsupervised[T](sc, schemaRDD)
}
