/*
package com.zsibio

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql._

import math._
import scala.util.Random
import scala.util.control.Breaks._

trait LDPruningMethods{
  def pairComposite (snp1: RDD[Int], snp2: RDD[Int]): Double
  def pairR (snp1: RDD[Int], snp2: RDD[Int]): Double
  def pairDPrime (snp1: RDD[Int], snp2: RDD[Int]): Double
  def pairCorr (snp1: RDD[Int], snp2: RDD[Int]): Double
 // def performLDPruning(dataSet: DataFrame, method: String, ldThreshold: Double, slideMaxBp: Int, slideMaxN: Int): DataFrame
  def performLDPruning(snpIdSet: Array[String], snpSet :List[RDD[Int]], method: String, ldThreshold: Double, slideMaxBp: Int, slideMaxN: Int): List[String]
}

@SerialVersionUID(15L)
class LDPruning[T] (sc: SparkContext, sqlContext: SQLContext, seq: RDD[Int]) extends Serializable with LDPruningMethods {

    private def packedCond (cond: (Int, Int) => Boolean, op: (Int, Int) => Int): Vector[Int] ={
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
      }).collect().toVector
    }

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
  private val validSNPxy: Vector[Int] = packedCond((x: Int, y: Int) => (x < 3 && y < 3), (x: Int, y:Int) => x * y)

  private val IncArray: Array[Array[Int]] = Array(
    Array(0, 0, 0, 2, 0), // BB, BB
    Array(0, 0, 1, 1, 0), // BB, AB
    Array(0, 0, 2, 0, 0), // BB, AA
    Array(0, 1, 0, 1, 0), // AB, BB
    Array(0, 0, 0, 0, 2), // AB, AB
    Array(1, 0, 1, 0, 0), // AB, AA
    Array(0, 2, 0, 0, 0), // AA, BB
    Array(1, 1, 0, 0, 0), // AA, AB
    Array(2, 0, 0, 0, 0)  // AA, AA
  )

  private val numAA: Vector[Int] = packedCond((x: Int, y: Int) => (x < 3 && y < 3), (x: Int, y: Int) => IncArray(x * 3 + y)(0))
  private val numAB: Vector[Int] = packedCond((x: Int, y: Int) => (x < 3 && y < 3), (x: Int, y: Int) => IncArray(x * 3 + y)(1))
  private val numBA: Vector[Int] = packedCond((x: Int, y: Int) => (x < 3 && y < 3), (x: Int, y: Int) => IncArray(x * 3 + y)(2))
  private val numBB: Vector[Int] = packedCond((x: Int, y: Int) => (x < 3 && y < 3), (x: Int, y: Int) => IncArray(x * 3 + y)(3))
  private val numDH2: Vector[Int] = packedCond((x: Int, y: Int) => (x < 3 && y < 3), (x: Int, y: Int) => IncArray(x * 3 + y)(4))


  def pairComposite (snp1: RDD[Int], snp2: RDD[Int]): Double ={
     val frequencies = snp1.zip(snp2).map{case (s1, s2) =>{
      val g1 = if (0 <= s1 && s1 <= 2) s1 | ~0x03 else 0xFF
      val g2 = if (0 <= s2 && s2 <= 2) s2 | ~0x03 else 0xFF
      val p = (((g1 & 0xFF) << 8) | (g2 & 0xFF))
      val q = (((g2 & 0xFF) << 8) | (g1 & 0xFF))
      (validNumSNP(p), numSNPaa(p), numSNPaA(p), numSNPAA(p), numSNPaa(q), numSNPaA(q), numSNPAA(q),
        numSNPAABB(p), numSNPaabb(p), numSNPaaBB(p), numSNPAAbb(p))
    }
  }.map(tup => List(tup._1, tup._2, tup._3, tup._4, tup._5, tup._6, tup._7,
    tup._8, tup._9, tup._10, tup._11)).collect.toList.transpose.map(vec => vec.sum)

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
        return delta / sqrt(t)
    }

    return Double.NaN
  }

  def pairCorr (snp1: RDD[Int], snp2: RDD[Int]): Double ={
    val frequencies = snp1.zip(snp2).map{
      case (s1, s2) =>{
        val g1 = if (0 <= s1 && s1 <= 2) s1 | ~0x03 else 0xFF
        val g2 = if (0 <= s2 && s2 <= 2) s2 | ~0x03 else 0xFF
        val p = (((g1 & 0xFF) << 8) | (g2 & 0xFF))
        val q = (((g2 & 0xFF) << 8) | (g1 & 0xFF))
        (validNumSNP(p), validSNPx(p), validSNPx(q), validSNPx2(p), validSNPx2(q), validSNPxy(p))
      }
    }.map(tup => List(tup._1, tup._2, tup._3, tup._4, tup._5, tup._6))
        .collect.toList.transpose.map(vec => vec.sum)

    val n  = frequencies(0); val X  = frequencies(1); val Y  = frequencies(2);
    val XX = frequencies(3); val YY = frequencies(4); val XY = frequencies(5);

    if (n > 0){
      val c1 : Double = XX - X * X / n.toDouble
      val c2 : Double = YY - Y * Y / n.toDouble
      val t  : Double = c1 * c2
      if (t > 0)
        return (XY - X * Y / n.toDouble) / sqrt(t)
    }

    return Double.NaN
  }

  private def epsilon (): Double ={
    lazy val s: Stream[Double] = 1.0 #:: s.map(f => f / 2.0)
    s.takeWhile(e => e + 1.0 != 1.0).last
  }

  private def pLog (value: Double): Double = return math.log(value + epsilon)

  private def proportionHaplo (nAA: Long, nAB: Long, nBA: Long, nBB: Long, nDH2: Long) : Map[String, Double] ={

    val EMFactor : Double = 0.01
    val nMaxIter : Int = 1000
    val funcRelTol : Double = sqrt(epsilon)

    val nTotal : Double = nAA + nAB + nBA + nBB + nDH2

    var proportions : Map[String, Double] = Map("pAA" -> .0, "pAB" -> .0, "pBA" -> .0, "pBB" -> .0)

    /* var (pAA, pAB, pBA, pBB) : (Double, Double, Double, Double) = (0., 0., 0., 0.) */
    /* def getFreq (p: Double, factor: Double, N: Double) : Double = return { (p + factor) / N } */

    if ((nTotal > 0) && (nDH2 > 0)){
      val div : Double = nAA + nAB + nBA + nBB + 4.0 * EMFactor
      proportions = (proportions.keySet, Array(nAA, nAB, nBA, nBB)).zipped.map((p, n) => (p, (n + EMFactor) / div)).toMap

      val nDH : Long = nDH2 / 2
      var logLH : Double =  (proportions.keySet, Array(nAA, nAB, nBA, nBB, nDH)).zipped.map((p, n) => n * pLog(proportions.get(p).get)).sum
      var conTol : Double = abs(funcRelTol * logLH)
      if (conTol < epsilon) conTol = epsilon

      breakable {for (i <- 1 until nMaxIter){
        val pAABB: Double = proportions.get("pAA").get * proportions.get("pBB").get
        val pABBA: Double = proportions.get("pAB").get * proportions.get("pBA").get

        val nDHAABB: Double = pAABB / (pAABB + pABBA) * nDH
        val nDHABBA: Double = pABBA / (pAABB + pABBA) * nDH

        proportions = (proportions.keySet, Array(nAA, nAB, nBA, nBB), Array(nDHAABB, nDHABBA, nDHABBA, nDHAABB)).zipped.map((p, n, nDH) => (p, (n + nDH) / nTotal)).toMap
        val newLogLH : Double =  (proportions.keySet, Array(nAA, nAB, nBA, nBB, nDH)).zipped.map((p, n) => n * pLog(proportions.get(p).get)).sum

        if (abs(logLH - newLogLH) <= conTol)
          break
        logLH = newLogLH
      }}
    } else{
      proportions = (proportions.keySet, Array(nAA, nAB, nBA, nBB)).zipped.map((p, n) => (p, n / nTotal)).toMap
    }

    return proportions
  }

  def pairR (snp1: RDD[Int], snp2: RDD[Int]): Double ={
    val frequencies = snp1.zip(snp2).map{
      case (s1, s2) => {
        val g1 = if (0 <= s1 && s1 <= 2) s1 | ~0x03 else 0xFF
        val g2 = if (0 <= s2 && s2 <= 2) s2 | ~0x03 else 0xFF
        val p = (((g1 & 0xFF) << 8) | (g2 & 0xFF))
        (numAA(p), numAB(p), numBA(p), numBB(p), numDH2(p))
      }
    }.map(tup => List(tup._1, tup._2, tup._3, tup._4, tup._5)).collect.toList.transpose.map(vec => vec.sum)

    val proportions : Map[String, Double] = this.proportionHaplo(frequencies(0),
      frequencies(1), frequencies(2), frequencies(3), frequencies(4))

    val pA  : Double = proportions.get("pAA").get + proportions.get("pAB").get
    val p_A : Double = proportions.get("pAA").get + proportions.get("pBA").get
    val pB  : Double = proportions.get("pBB").get + proportions.get("pBA").get
    val p_B : Double = proportions.get("pBB").get + proportions.get("pAB").get
    val D   : Double = proportions.get("pAA").get - pA  * p_A

    return (D / sqrt(pA * p_A * pB * p_B))
  }

  def pairDPrime (snp1: RDD[Int], snp2: RDD[Int]): Double ={
    val frequencies = snp1.zip(snp2).map{
      case (s1, s2) =>{
        val g1 = if (0 <= s1 && s1 <= 2) s1 | ~0x03 else 0xFF
        val g2 = if (0 <= s2 && s2 <= 2) s2 | ~0x03 else 0xFF
        val p = (((g1 & 0xFF) << 8) | (g2 & 0xFF))
        (numAA(p), numAB(p), numBA(p), numBB(p), numDH2(p))
      }
    }.map(tup => List(tup._1, tup._2, tup._3, tup._4, tup._5)).collect.toList.transpose.map(vec => vec.sum)

    val proportions : Map[String, Double] = this.proportionHaplo(frequencies(0),
      frequencies(1), frequencies(2), frequencies(3), frequencies(4))

    val pA  : Double = proportions.get("pAA").get + proportions.get("pAB").get
    val p_A : Double = proportions.get("pAA").get + proportions.get("pBA").get
    val pB  : Double = proportions.get("pBB").get + proportions.get("pBA").get
    val p_B : Double = proportions.get("pBB").get + proportions.get("pAB").get
    val D   : Double = proportions.get("pAA").get - pA  * p_A
    val donominator : Double = if (D >= 0) min(pA * p_B, pB * p_A) else max(-pA * p_A, -pB * p_B)

    return (D / donominator)
  }

  private def calcLD(method: String, snp1: RDD[Int], snp2: RDD[Int]) : Double = {
    method match {
      case "composite" => return pairComposite(snp1, snp2)
      case "r"         => return pairR(snp1, snp2)
      case "dprime"    => return pairDPrime(snp1, snp2)
      case "corr"      => return pairCorr(snp1, snp2)
    }
    return Double.NaN
  }

  /*def performLDPruning(dataSet: DataFrame, method: String, ldThreshold: Double, slideMaxBp: Int, slideMaxN: Int): DataFrame ={
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
      val snpI = dataSet.select(snpsetId(i)).rdd.map(row => row.toSeq.map(x = > x.asInstanceOf[Int]))//.collect().map(_.getInt(0)).toVector
      val posI = snpsetId(i).split(":")(1).toInt
      for (j <- 1 until outputDataSet.columns.size){
        totalCnt += 1
        println(totalCnt)
        val snpJId = outputDataSet.columns(j)
        val posJ = snpJId.split(":")(1).toInt
        if ((math.abs(i - j) <= slideMaxN) && (math.abs(posI - posJ) <= slideMaxBp)){
          val snpJ = outputDataSet.select(snpJId).rdd//.collect().map(_.getInt(0)).toVector
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
  }*/


  case class TSNP(snpIdx: Int, snpId: String, snpPos: Int, snp: RDD[Int]){
    def this(snpIdx: Int, snpId: String, snpPos: Int) = this(snpIdx, snpId, snpPos, null)
  }

  def performLDPruning(snpIdSet: Array[String], snpSet :List[RDD[Int]], method: String = "composite", ldThreshold: Double = 0.2, slideMaxBp: Int = 500000, slideMaxN: Int = Int.MaxValue): List[String] ={
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
          if ((abs(i - vec.snpIdx) <= slideMaxN) && (abs(posI - vec.snpPos) <= slideMaxBp)) {
            if (abs(calcLD(method, snpSet(i), vec.snp)) <= ldThreshold)
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
        if ((abs(snp.snpIdx - startIdx) <= slideMaxN) && (abs(snp.snpPos - posStartIdx)) <= slideMaxBp)
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
          if ((abs(i - vec.snpIdx) <= slideMaxN) && (abs(posI - vec.snpPos) <= slideMaxBp)) {
            if (abs(calcLD(method, snpSet(i), vec.snp)) <= ldThreshold)
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
  def apply[T](sc: SparkContext, sqlContext: SQLContext, seq: RDD[Int]): LDPruning[T] = new LDPruning[T](sc, sqlContext, seq)
}

    Contact GitHub API Training Shop Blog About 

    © 2016 GitHub, Inc. Terms Privacy Security Status Help 

*/
