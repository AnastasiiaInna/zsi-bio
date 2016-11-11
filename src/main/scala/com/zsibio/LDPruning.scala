package com.zsibio

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import scala.util.Random

trait LDPruningMethods{
  def performLDPruning(dataSet: DataFrame, method: String, ldThreshold: Double, slideMaxBp: Int, slideMaxN: Int): DataFrame
  def performLDPruning(sortedVariantsBySampelId: RDD[(String, Array[SampleVariant])], method: String, ldThreshold: Double, slideMaxBp: Int, slideMaxN: Int): List[String]
}

@SerialVersionUID(15L)
class LDPruning[T] (sc: SparkContext, sqlContext: SQLContext) extends Serializable with LDPruningMethods {

  private def packedCond (cond: (Int, Int) => Boolean, op: (Int, Int) => Int): Vector[Int] ={
    val _size = 256 * 256
    val seq = (0 until _size)

    seq.map(s => {
      var g1: Int = s / 256
      var g2: Int = s % 256
      var sum: Int = 0

      var i = 0
      for (i <- 0 until 4) {
        val b1: Int = g1 & 0x03
        val b2: Int = g2 & 0x03

        if (cond(b1, b2)) sum += op(b1, b2)
        g1 >>= 2
        g2 >>= 2
      }
      sum
    }).toVector
  }

  private val _size: Int = 256*256;

  private val validNumSNP: Vector[Int] = packedCond((x: Int, y: Int) => (x < 3 && y < 3), (x: Int, Int) => 1)
  // The number of aa in a pair of packed SNPs:
  private val numSNPaa: Vector[Int] = packedCond((x: Int, y: Int) => (x == 0 && y < 3), (x: Int, Int) => 1)
  private val numSNPaA: Vector[Int] = packedCond((x: Int, y: Int) => (x == 1 && y < 3), (x: Int, Int) => 1)
  private val numSNPAA: Vector[Int] = packedCond((x: Int, y: Int) => (x == 2 && y < 3), (x: Int, Int) => 1)

  private val numSNPAABB: Vector[Int] = packedCond((x: Int, y: Int) => (x == 2 && y == 2), (x: Int, Int) => 1)
  private val numSNPaabb: Vector[Int] = packedCond((x: Int, y: Int) => (x == 0 && y == 0), (x: Int, Int) => 1)
  private val numSNPaaBB: Vector[Int] = packedCond((x: Int, y: Int) => (x == 0 && y == 2), (x: Int, Int) => 1)
  private val numSNPAAbb: Vector[Int] = packedCond((x: Int, y: Int) => (x == 2 && y == 0), (x: Int, Int) => 1)

  private val validSNPx: Vector[Int] = packedCond((x: Int, y: Int) => (x < 3 && y < 3), (x: Int, Int) => x)
  private val validSNPx2: Vector[Int] = packedCond((x: Int, y: Int) => (x < 3 && y < 3), (x: Int, Int) => x * x)
  private val validSNPy: Vector[Int] = packedCond((x: Int, y: Int) => (x < 3 && y < 3), (x: Int, y:Int) => x * y)


  def pairComposite (snp1: Vector[Int], snp2: Vector[Int]): Double ={
    val frequencies = (snp1, snp2).zipped.map{
      (s1, s2) =>{
        val g1 = if (0 <= s1 && s1 <= 2) s1 | ~0x03 else 0xFF
        val g2 = if (0 <= s2 && s2 <= 2) s2 | ~0x03 else 0xFF
        val p = (((g1 & 0xFF) << 8) | (g2 & 0xFF))
        val q = (((g2 & 0xFF) << 8) | (g1 & 0xFF))
        (validNumSNP(p), numSNPaa(p), numSNPaA(p), numSNPAA(p), numSNPaa(q), numSNPaA(q), numSNPAA(q),
          numSNPAABB(p), numSNPaabb(p), numSNPaaBB(p), numSNPAAbb(p))
      }
    }.map(tup => List(tup._1, tup._2, tup._3, tup._4, tup._5, tup._6, tup._7,
      tup._8, tup._9, tup._10, tup._11)).transpose.map(vec => vec.sum)

    val n = frequencies(0); val naa = frequencies(1);  val naA = frequencies(2);
    val nAA = frequencies(3); val nbb = frequencies(4); val nbB = frequencies(5);
    val nBB = frequencies(6); val nAABB = frequencies(7); val naabb = frequencies(8);
    val naaBB = frequencies(9); val nAAbb = frequencies(10)

    if (n > 0){
      val delta: Double = (nAABB + naabb - naaBB - nAAbb) / (2.0 * n) - (naa - nAA) * (nbb - nBB) / (2.0 * n * n)
      val pa: Double = (2 * naa + naA) / (2.0 * n)
      val pA: Double = 1 - pa
      val pAA: Double = nAA / n.toDouble
      val pb: Double = (2 * nbb + nbB) / (2.0 * n)
      val pB: Double = 1 - pb
      val pBB: Double = nBB / n.toDouble
      val DA: Double = pAA - pA * pA
      val DB: Double = pBB - pB * pB
      val t: Double = (pA * pa + DA) * (pB * pb + DB)
      if (t > 0)
        return delta / math.sqrt(t)
    }

    return Double.NaN
  }

  private def calcLD(method: String, snp1: Vector[Int], snp2: Vector[Int]) : Double = {
    method match {
      case "composite" => pairComposite(snp1, snp2)
    }
    return Double.NaN
  }

  def performLDPruning(dataSet: DataFrame, method: String, ldThreshold: Double, slideMaxBp: Int, slideMaxN: Int): DataFrame ={

    val snpsetId = dataSet.columns
    val rnd = new Random
    val snpNumber: Int = snpsetId.size
    val startIdx: Int  = rnd.nextInt(snpNumber - 3) + 2

    var outputDataSet = dataSet.select("SampleId", snpsetId(startIdx))
    val posStartIdx = snpsetId(startIdx).split(":")(1).toInt

    println(startIdx)
    println(posStartIdx)
    println(outputDataSet.columns.size)

    // increasing searching: i = i + 1

    var i: Int = startIdx + 1
    for (i <- startIdx + 1 until snpNumber){
      var j: Int = 0
      var validCnt: Int = 0
      var totalCnt: Int = 0
      val snpI = dataSet.select(snpsetId(i)).rdd.collect().map(_.getInt(0)).toVector
      val posI = snpsetId(i).split(":")(1).toInt

      for (j <- 1 until outputDataSet.columns.size){
        totalCnt += 1
        println(totalCnt)
        val snpJId = outputDataSet.columns(j)
        val posJ = snpJId.split(":")(1).toInt

        if ((math.abs(i - j) <= slideMaxN) && (math.abs(posI - posJ) <= slideMaxBp)){
          val snpJ = outputDataSet.select(snpJId).collect().map(_.getInt(0)).toVector
          if (math.abs(pairComposite(snpI, snpJ)) <= ldThreshold)
            validCnt += 1
        }
        else{
          validCnt += 1
          outputDataSet = outputDataSet.drop(outputDataSet.columns(j))
        }
      }
      if (totalCnt == validCnt)
        outputDataSet = outputDataSet.join(dataSet.select("SampleId", snpsetId(i)), "SampleId")
    }

    println(outputDataSet.columns.size)
    println("Increase is ok")

    // decreasing searching: i = i - 1

    var outputDataSetDec = outputDataSet

    val snpSetToDrop = (startIdx + 1 until snpNumber).map{k =>
      if ((math.abs(k - startIdx) > slideMaxN) || (math.abs(outputDataSetDec.columns(k).split(":")(1).toInt - posStartIdx) > slideMaxBp))
        snpsetId(k)
    }.map(snpId => snpId.toString)

    for(i <- 0 until snpSetToDrop.size)
      outputDataSetDec = outputDataSet.drop(snpSetToDrop(i))

    /*    for (i <- startIdx + 1 until snpNumber){
          if ((math.abs(i - startIdx) > slideMaxN) || (math.abs(snpsetId(i).toInt - outputDataSet.columns(startIdx).toInt) > slideMaxBp))
            outputDataSetDec = outputDataSetDec.drop(snpsetId(i))
        }*/

    for (i <- 2 until startIdx){
      var j: Int = 0
      var validCnt: Int = 0
      var totalCnt: Int = 0
      val snpI = dataSet.select(snpsetId(i)).rdd.collect().map(_.getInt(0)).toVector
      val posI = snpsetId(i).split(":")(1).toInt

      for (j <- 1 until outputDataSetDec.columns.size){
        totalCnt += 1
        val snpJId = outputDataSet.columns(j)
        val posJ = snpJId.split(":")(1).toInt

        if ((math.abs(i - j) <= slideMaxN) && (math.abs(posI - posJ) <= slideMaxBp)){
          val snpJ = outputDataSetDec.select(snpJId).rdd.collect().map(_.getInt(0)).toVector
          if (math.abs(pairComposite(snpI, snpJ)) <= ldThreshold)
            validCnt += 1
        }
        else{
          validCnt += 1
          outputDataSetDec = outputDataSetDec.drop(outputDataSetDec.columns(j))
        }
      }
      if (totalCnt == validCnt)
        outputDataSetDec = outputDataSetDec.join(dataSet.select("SampleId", snpsetId(i)), "SampleId")

    }

    outputDataSet = outputDataSet.join(outputDataSetDec)
    return  outputDataSet
  }


  case class TSNP(snpIdx: Int, snpId: String, snpPos: Int, snp: Vector[Int]){
    def this(snpIdx: Int, snpId: String, snpPos: Int) = this(snpIdx, snpId, snpPos, null)
  }

  def performLDPruning(sortedVariantsBySampelId: RDD[(String, Array[SampleVariant])], method: String = "composite", ldThreshold: Double = 0.2, slideMaxBp: Int = 500000, slideMaxN: Int = Int.MaxValue): List[String] ={

    val snpIdSet: Array[String] = sortedVariantsBySampelId.first()._2.map(_.variantId.toString)
    val snpSet: List[Vector[Int]] = sortedVariantsBySampelId.map {
      case (_, sortedVariants) =>
        sortedVariants.map(_.alternateCount.toInt)
    }.zipWithIndex().flatMap{
      case (row, rowIndex) => row.zipWithIndex.map {
        case (number, columnIndex) => columnIndex -> (rowIndex, number)
      }
    }.groupByKey.sortByKey().values.map {
      indexedRow => indexedRow.toSeq.sortBy(_._1).map(_._2).toVector
    }.collect().toList

    val rnd = new Random
    val snpNumber: Int = snpIdSet.size
    val startIdx: Int  = rnd.nextInt(snpNumber - 1)
    val posStartIdx = snpIdSet(startIdx).split(":")(1).toInt

    var listGeno: List[TSNP] = List(new TSNP(startIdx, snpIdSet(startIdx), posStartIdx, snpSet(startIdx)))
    var outputSNPIdSet: List[TSNP] = listGeno

    // increasing searching: i = i + 1

    var i: Int = startIdx + 1

    for (i <- startIdx + 1 until snpNumber) {
      var validCnt: Int = 0
      var totalCnt: Int = 0
      val posI = snpIdSet(i).split(":")(1).toInt

      listGeno.foreach{
        vec =>{
          totalCnt += 1
          if ((math.abs(i - vec.snpIdx) <= slideMaxN) && (math.abs(posI - vec.snpPos) <= slideMaxBp)) {
            if (math.abs(pairComposite(snpSet(i), vec.snp)) <= ldThreshold)
              validCnt += 1
          }
          else{
            validCnt += 1
            listGeno = listGeno.patch(listGeno.indexOf(vec), Nil, 1)
          }
        }
      }
      if (totalCnt == validCnt) {
        listGeno ::= new TSNP(i, snpIdSet(i), posI, snpSet(i))
        outputSNPIdSet ::= new TSNP(i, snpIdSet(i), posI, snpSet(i))
      }
    }

    // decreasing searching: i = i - 1

    listGeno = Nil
    outputSNPIdSet.foreach{
      snp => {
        if ((math.abs(snp.snpIdx - startIdx) <= slideMaxN) && (math.abs(snp.snpPos - posStartIdx)) <= slideMaxBp)
          listGeno ::= snp
      }
    }

    for (i <- 0 until startIdx) {
      var validCnt: Int = 0
      var totalCnt: Int = 0
      val posI = snpIdSet(i).split(":")(1).toInt

      listGeno.foreach{
        vec =>{
          totalCnt += 1
          if ((math.abs(i - vec.snpIdx) <= slideMaxN) && (math.abs(posI - vec.snpPos) <= slideMaxBp)) {
            if (math.abs(pairComposite(snpSet(i), vec.snp)) <= ldThreshold)
              validCnt += 1
          }
          else{
            validCnt += 1
            listGeno = listGeno.patch(listGeno.indexOf(vec), Nil, 1)
          }
        }
      }
      if (totalCnt == validCnt) {
        listGeno ::= new TSNP(i, snpIdSet(i), posI, snpSet(i))
        outputSNPIdSet ::= new TSNP(i, snpIdSet(i), posI, snpSet(i))
      }
    }

    val snpIdSetPrunned : List[String] = outputSNPIdSet.map(snp => snp.snpId)

    return snpIdSetPrunned
  }
}

object LDPruning {
  def apply[T](sc: SparkContext, sqlContext: SQLContext): LDPruning[T] = new LDPruning[T](sc, sqlContext)
}
