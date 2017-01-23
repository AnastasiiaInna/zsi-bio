/*
import org.apache.spark.mllib.linalg.LinalgShim._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import breeze.linalg.norm

import smile.mds.MDS
import smile.projection.PCA
import smile.math.distance

def mds(proximity: Array[Array[Double]], k: Int, add: Boolean = false) = new MDS(proximity, k, add)


val matr = Array(Array(0.0, 2.0, 3.4),
                 Array(3.0, 0.0, 4.5),
                 Array(1.2, 3.4, 0.0))

val model = mds(matr, 2)
val variance = model.getProportion.toList
val numberPC = variance.filter(value => value >= 0.7).length
val pc = model.getCoordinates.map(arr => arr.slice(0, numberPC).toList).toList

def pca(data: Array[Array[Double]], cor: Boolean = false): PCA = new PCA(data, cor)
pca(matr).getVarianceProportion.toList
pca(matr).getLoadings.toList.map(x => x.toList)
pca(matr).getProjection.toList.map(x => x.toList)

def computeEuclidean(v1: Vector, v2: Vector): Double = {
  val b1 = toBreeze(v1)
  val b2 = toBreeze(v2)
  norm((b1 - b2), 2.0)
}


import org.apache.spark.sql._
import org.apache.spark.sql.types._

val schema = StructType(Seq(
  StructField("a", DoubleType, false),
  StructField("b", DoubleType, false)))



//val incl = inclusive(11, 12)
//incl.map(print)
//print(incl.size)
//
//
//val line = "aldk wekwoe pfwekfopkw owekfop wefkwpf"
//val splittedLine = line.split(" ").toList
//new Tuple3(splittedLine(0), splittedLine(1), splittedLine(2))
//
///*def extract(file: String, filter: (String, String, String) => Boolean): Map[String, String, String] = {
//  Source.fromFile(file).getLines().map(line => {
//    val tokens = line.split("\t").toList
//    new Tuple3(tokens(0), tokens(1), tokens(2))
//  }).toMap.filter(tuple => filter(tuple._1, tuple._2, tuple._3))
//}
//
//val panelFile = "/home/anastasiia/1000genomes/ALL.panel"
//val panel = Source.fromFile(panelFile).getLines().map(line => {val tokens = line.split("\t").toList
//              new Tuple3(tokens(0), tokens(1), tokens(2))})
//
//print(panel.toList.toString())
//
//
//val extractPanel = panel.toList.map(_._3 == "EUR")
//
//print(extractPanel.toString())
//
//
//
//val populations = Array( "BEB")
//
//def extract(file: String, filter: (String, String) => Boolean): Map[String, (String, String)] = {
//  Source.fromFile(file).getLines().map(line => {
//    val tokens = line.split("\t").toList
//      tokens(0) -> (tokens(1) -> tokens(2))
//  }
//  ).toMap.filter(tuple => filter(tuple._1, tuple._2._1))
//}
//
//val panel = extract(panelFile, (sampleID: String, pop: String) => populations.contains(pop))
//
//
//panel.groupBy(_._1).size
//
//// panel.getOrElse("HG00139", "Uknown")
//
//
//print(panel.toString())
//
//
//val xs =  (0 until 100)
// val ys =  (3 until 103)
//val p = Plot().withScatter(xs, ys)
//draw(p, "basic-scatter", writer.FileOptions(overwrite=true))
//
//val arr:Array[Int] = Array(1, 2, 3)
//arr.toList.filter(_>1).size
//
//val x = Array(Vector.range(1, 5), Vector.range(1, 5))
//print(x.toList)
//
//math.max(2,3 )
//
//val permittedRange = inclusive(0, 3)
//print(permittedRange)
//permittedRange.contains(0)
//
//val alfa = 10
//val betta = 2
//val gamma = 90
//
//s"$alfa:$betta:$gamma"
//
//(0 until 4)*/



/*
def packedCond (cond: (Int, Int) => Boolean, op: (Int, Int) => Int): Vector[Int] ={
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

val _size: Int = 256*256;


val validNumSNP: Vector[Int] = packedCond((x: Int, y: Int) => (x < 3 && y < 3), (x: Int, Int) => 1)
// The number of aa in a pair of packed SNPs:
val numSNPaa: Vector[Int] = packedCond((x: Int, y: Int) => (x == 0 && y < 3), (x: Int, Int) => 1)
val numSNPaA: Vector[Int] = packedCond((x: Int, y: Int) => (x == 1 && y < 3), (x: Int, Int) => 1)
val numSNPAA: Vector[Int] = packedCond((x: Int, y: Int) => (x == 2 && y < 3), (x: Int, Int) => 1)

val numSNPAABB: Vector[Int] = packedCond((x: Int, y: Int) => (x == 2 && y == 2), (x: Int, Int) => 1)
val numSNPaabb: Vector[Int] = packedCond((x: Int, y: Int) => (x == 0 && y == 0), (x: Int, Int) => 1)
val numSNPaaBB: Vector[Int] = packedCond((x: Int, y: Int) => (x == 0 && y == 2), (x: Int, Int) => 1)
val numSNPAAbb: Vector[Int] = packedCond((x: Int, y: Int) => (x == 2 && y == 0), (x: Int, Int) => 1)

val validSNPx: Vector[Int] = packedCond((x: Int, y: Int) => (x < 3 && y < 3), (x: Int, Int) => x)
val validSNPx2: Vector[Int] = packedCond((x: Int, y: Int) => (x < 3 && y < 3), (x: Int, Int) => x * x)
val validSNPy: Vector[Int] = packedCond((x: Int, y: Int) => (x < 3 && y < 3), (x: Int, y:Int) => x * y)


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






val snp1 = Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0)
val snp2 = Vector(0, 2, 0, 2, 2, 1, 1, 1, 0, 0, 2, 0, 2, 2, 1, 1, 1, 3)

/*val snp1 = Vector(2, 0, 1)
val snp2 = Vector(0, 2, 0)*/


val frequencies = (snp1, snp2).zipped.map{
  (s1, s2) =>{
    val g1 = if (0 <= s1 && s1 <= 2) s1 | ~0x03 else 0xFF
    val g2 = if (0 <= s2 && s2 <= 2) s2 | ~0x03 else 0xFF
    val p = (((g1 & 0xFF) << 8) | (g2 & 0xFF))
    val q = (((g2 & 0xFF) << 8) | (g1 & 0xFF))
    // val p = s1 << 8 | s2
    // val q = s2 << 8 | s1
    (validNumSNP(p), numSNPaa(p), numSNPaA(p), numSNPAA(p), numSNPaa(q), numSNPaA(q), numSNPAA(q),
      numSNPAABB(p), numSNPaabb(p), numSNPaaBB(p), numSNPAAbb(p))
  }
}.map(tup => List(tup._1, tup._2, tup._3, tup._4, tup._5, tup._6, tup._7,
  tup._8, tup._9, tup._10, tup._11)).transpose.map(vec => vec.sum)

val n = frequencies(0); val naa = frequencies(1);  val naA = frequencies(2);
val nAA = frequencies(3); val nbb = frequencies(4); val nbB = frequencies(5);
val nBB = frequencies(6); val nAABB = frequencies(7); val naabb = frequencies(8);
val naaBB = frequencies(9); val nAAbb = frequencies(10)


  val delta: Double = (nAABB + naabb - naaBB - nAAbb) / (2.0 * n) - (naa - nAA) * (nbb - nBB) / (2.0 * n * n)
  val pa: Double = (2 * naa + naA) / (2.0 * n)
  val pA: Double = 1 - pa
  val pAA: Double = nAA / n.toDouble
  val pb: Double = (2 * nbb + nbB) / (2.0 * n)
  val pB: Double = 1 - pb
  val pBB: Double = nBB / n.toDouble
  val DA: Double = pAA - pA * pA
  val DB: Double = pBB - pB * pB
  val tt: Double = (pA * pa + DA) * (pB * pb + DB)

  delta / math.sqrt(tt)


pairComposite(snp1, snp2)

val s = (0 until 4)
val t = s.map(ss => {
  val x: Int = ss
  val y: Int = ss + 1
  val z: Int = ss + 2
  (x, y, z, z)
}).map(t => List(t._1, t._2, t._3, t._4)).transpose.map(f => f.sum)


private def calcLD(method: String, snp1: Vector[Int], snp2: Vector[Int]) : Double = {
  method match {
    case "composite" => pairComposite(snp1, snp2)
  }
  return Double.NaN
}

var N = 3
var j = 0
while (j < N){
  N += 1
  println(N)
  j += 2
}

val reg = "\\:(d+)\\:".r
val temp = "s345sdfx:092:3"

val pos = temp.split(":")
pos(1).toInt

val ifTrue = (2 > Double.NaN.toInt)

var l = List("a", "v", "s")

l.size
l.length
// l ::=  Vector(4, 5, 6)

var a = 1
l.foreach{
  vec =>{
    a += 1
    println(a)
  }
}



val i = l.indexOf(5)

(0 until 10).map{x => a += x}

l ++ List("3", "4")

// match {case (x, y, z, w) => (x.sum, y.sum, z.sum, w.sum)}

/*
  if (n > 0)
  {
    double delta =
      double(nAABB + naabb - naaBB - nAAbb) / (2*n) -
        double(naa-nAA)*double(nbb-nBB) / (2.0*n*n);

    double pa = double(2*naa + naA) / (2*n);
    double pA = 1 - pa, pAA = double(nAA)/n;
    double pb = double(2*nbb + nbB) / (2*n);
    double pB = 1 - pb, pBB = double(nBB)/n;
    double DA = pAA - pA*pA;
    double DB = pBB - pB*pB;
    double t = (pA*pa + DA) * (pB*pb + DB);
    if (t > 0)
      return delta / sqrt(t);
  }
  return R_NaN;
}
*/

var (xxx, yyy) : (Double, Double) = (90.,10.)

var sss = Map("xxx" -> 0.0, "yyy" -> 0.0)
sss = (sss.keySet, Array(xxx, yyy)).zipped.map((a, b) => (a, b + 1)).toMap
sss.get("xxx").get + sss.get("yyy").get
*/
*/
