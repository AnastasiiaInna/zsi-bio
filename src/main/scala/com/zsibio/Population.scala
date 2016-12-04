package com.zsibio

import org.apache.spark.{SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.bdgenomics.formats.avro.{Genotype, GenotypeAllele}

import scala.collection.JavaConverters._

case class SampleVariant(sampleId: String, variantId: String, alternateCount: Int)

trait PopulationMethods{
  def getDataSet(dataSet: RDD[(String, Array[SampleVariant])], prunnedSnpIdSet: List[String]): DataFrame
  def getDataSet(dataSet: RDD[(String, Array[Double])]): DataFrame
}

@SerialVersionUID(15L)
class Population[T] (sc: SparkContext, sqlContext: SQLContext, genotypes: RDD[Genotype], panel: Map[String, String],
                     val missingRate: Double = 0.0, var infFreq: Double = 0.05, var supFreq: Double = 1.0) extends Serializable with PopulationMethods{

  private var _infFreq     : Double = infFreq
  private var _supFreq     : Double = supFreq
  private val _missingRate : Double = missingRate
  var _dataSet: DataFrame = null
  var _numberOfRegions : Int = 0

  def infFreq_ (value: Double): Unit = _infFreq = value
  def supFreq_ (value: Double): Unit = _supFreq = value

  private def variantId(genotype: Genotype): String = {
    val name = genotype.getVariant.getContig.getContigName
    val start = genotype.getVariant.getStart
    val end = genotype.getVariant.getEnd
    s"$name:$start:$end"
  }

  private def altCount(genotype: Genotype): Int = {
    genotype.getAlleles.asScala.count(_ != GenotypeAllele.Ref)
  }

  private def toVariant(genotype: Genotype): SampleVariant = {
    new SampleVariant(genotype.getSampleId.intern(), variantId(genotype), altCount(genotype))
  }

  val variantsRDD: RDD[SampleVariant] = genotypes.map(toVariant)

  private val variantsBySampleId: RDD[(String, Iterable[SampleVariant])] = variantsRDD.groupBy(_.sampleId).cache()

  val sampleCount: Long = variantsBySampleId.count()

  private val variantsByVariantId: RDD[(String, Iterable[SampleVariant])] = variantsRDD.groupBy(_.variantId).filter {
    case (_, sampleVariants) => sampleVariants.size <= (sampleCount * (1 + _missingRate)).toInt
  }

  private val variantFrequencies: RDD[(String, Double)] = variantsByVariantId.map {
    case (variantId, sampleVariants) => (variantId, sampleVariants.count(_.alternateCount > 0) / sampleCount.toDouble)
  }

  private val frequencies : Array[String] = if (_infFreq == .0 && _supFreq == 1.0) variantFrequencies.keys.collect else variantFrequencies.filter{case(_, freq) => freq >= _infFreq && freq <= _supFreq}.keys.collect()
  private val filteredVariants: RDD[(String, Iterable[SampleVariant])] = variantsRDD.filter{sampleVariant => frequencies contains(sampleVariant.variantId)}.groupBy(_.sampleId).cache()
  val sortedVariantsBySampelId: RDD[(String, Array[SampleVariant])] = filteredVariants.map{case (sampleId, sampleVariant) => (sampleId, sampleVariant.toArray.sortBy(_.variantId))}.cache()

  def getDataSet(sortedVariantsBySampleId: RDD[(String, Array[SampleVariant])] = sortedVariantsBySampelId, prunnedSnpIdSet: List[String] = null) : DataFrame ={
     val header = StructType(
      Array(StructField("SampleId", StringType)) ++
        Array(StructField("Region", StringType)) ++
        sortedVariantsBySampleId.first()._2.map(variant => {
          StructField(variant.variantId.toString, DoubleType)
        }))

     val rowRDD: RDD[Row] = sortedVariantsBySampleId.map {
       case (sampleId, sortedVariants) =>
         val region: Array[String] = Array(panel.getOrElse(sampleId, "Unknown"))
         val alternateCounts: Array[Double] = sortedVariants.map(_.alternateCount.toDouble)
         Row.fromSeq(Array(sampleId) ++ region ++ alternateCounts)
     }

     val dataSet: DataFrame = sqlContext.createDataFrame(rowRDD, header)//.toDF(header.fieldNames : _*)
     var outputDataSet: DataFrame = null

     if (prunnedSnpIdSet == null) outputDataSet = dataSet
     else{
       val columnsToSelect: List[String] = List("SampleId", "Region") ++ prunnedSnpIdSet
       outputDataSet = dataSet.select(columnsToSelect.head, columnsToSelect.tail: _*)
     }

     _dataSet = outputDataSet
     _numberOfRegions = outputDataSet.select("Region").distinct().count().toInt

     return outputDataSet
  }

  def getDataSet(variants: RDD[(String, Array[Double])]) : DataFrame ={
    val pc = Array.fill(variants.first()._2.length)("PC")
    val s = (1 until variants.first()._2.length)
    val headerPC = (pc, s).zipped.par.map{case(pc, s) => pc + s}

    val header = StructType(
      Array(StructField("SampleId", StringType)) ++
        Array(StructField("Region", StringType)) ++
        headerPC.map(pc => {StructField(pc, DoubleType)})
    )

    val rowRDD: RDD[Row] = variants.map {
      case (sampleId, variant) =>
        val region: Array[String] = Array(panel.getOrElse(sampleId, "Unknown"))
        Row.fromSeq(Array(sampleId) ++ region ++ variant)
    }

    val outputDataSet: DataFrame = sqlContext.createDataFrame(rowRDD, header)//.toDF(header.fieldNames : _*)

    _dataSet = outputDataSet
    _numberOfRegions = outputDataSet.select("Region").distinct().count().toInt

    return outputDataSet
  }

}

object Population {
  def apply[T](sc: SparkContext, sqlContext: SQLContext, genotypes: RDD[Genotype], panel: Map[String, String],
               missingRate: Double, infFreq: Double,  supFreq: Double): Population[T] = new Population[T](sc, sqlContext, genotypes, panel, missingRate, infFreq, supFreq)
}
