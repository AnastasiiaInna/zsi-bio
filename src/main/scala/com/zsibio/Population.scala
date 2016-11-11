package com.zsibio

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{Genotype, GenotypeAllele}

import scala.collection.JavaConverters._
import java.nio.file.{Files, Paths}

case class SampleVariant(sampleId: String, variantId: String, alternateCount: Int)

trait PopulationMethods{
  def getDataSet(dataSet: RDD[(String, Array[SampleVariant])], prunnedSnpIdSet: List[String]): DataFrame
}

@SerialVersionUID(15L)
class Population[T] (sc: SparkContext, sqlContext: SQLContext, genotypes: RDD[Genotype], panel: Map[String, String], var infFreq: Double = 0.05, var supFreq: Double = 1.0) extends Serializable with PopulationMethods{

  private var _infFreq : Double = infFreq
  private var _supFreq : Double = supFreq
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

  private def alternateCount(genotype: Genotype): Int = {
    genotype.getAlleles.asScala.count(_ != GenotypeAllele.Ref)
  }

  private def toVariant(genotype: Genotype): SampleVariant = {
    new SampleVariant(genotype.getSampleId.intern(), variantId(genotype), alternateCount(genotype))
  }

  val variantsRDD: RDD[SampleVariant] = genotypes.map(toVariant)

  private val variantsBySampleId: RDD[(String, Iterable[SampleVariant])] = variantsRDD.groupBy(_.sampleId).cache()

  val sampleCount: Long = variantsBySampleId.count()

  private val variantsByVariantId: RDD[(String, Iterable[SampleVariant])] = variantsRDD.groupBy(_.variantId).filter {
    case (_, sampleVariants) => sampleVariants.size == sampleCount
  }

  private val variantFrequencies: Map[String, Double] = variantsByVariantId.map {
    case (variantId, sampleVariants) => (variantId, sampleVariants.count(_.alternateCount > 0) / sampleCount.toDouble)
  }.collect().toMap

  val sortedVariantsBySampelId: RDD[(String, Array[SampleVariant])] ={
    val filteredVariantsBySampleId: RDD[(String, Iterable[SampleVariant])] = variantsBySampleId.map {
      case (sampleId, sampleVariants) =>
        val filteredSampleVariants = sampleVariants.filter(variant => variantFrequencies.getOrElse(variant.variantId, -1.0) >= _infFreq
          && variantFrequencies.getOrElse(variant.variantId, -1.0) <= _supFreq)
        (sampleId, filteredSampleVariants)
    }

    filteredVariantsBySampleId.map {case (sampleId, variants) => (sampleId, variants.toArray.sortBy(_.variantId)) }
  }

   def getDataSet(sortedVariantsBySampleId: RDD[(String, Array[SampleVariant])] = sortedVariantsBySampelId, prunnedSnpIdSet: List[String] = null) : DataFrame ={
     val header = StructType(
      Array(StructField("SampleId", StringType)) ++
        Array(StructField("Region", StringType)) ++
        sortedVariantsBySampleId.first()._2.map(variant => {
          StructField(variant.variantId.toString, IntegerType)
        }))

     val rowRDD: RDD[Row] = sortedVariantsBySampleId.map {
       case (sampleId, sortedVariants) =>
         val region: Array[String] = Array(panel.getOrElse(sampleId, "Unknown"))
         val alternateCounts: Array[Int] = sortedVariants.map(_.alternateCount)
         Row.fromSeq(Array(sampleId) ++ region ++ alternateCounts)
     }

     var dataSet: DataFrame = sqlContext.createDataFrame(rowRDD, header)
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

}

object Population {
  def apply[T](sc: SparkContext, sqlContext: SQLContext, genotypes: RDD[Genotype], panel: Map[String, String], infFreq: Double,  supFreq: Double): Population[T] = new Population[T](sc, sqlContext, genotypes, panel, infFreq, supFreq)
}
