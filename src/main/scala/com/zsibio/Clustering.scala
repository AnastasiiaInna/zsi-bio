package com.zsibio

/**
  * Created by anastasiia on 10/6/16.

  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{Genotype, GenotypeAllele}
import java.nio.file.{Files, Paths}

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.mllib.linalg.{Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

import scala.util.Random
// import org.apache.spark.mllib.linalg.{LinalgShim, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, RowMatrix}
import breeze.linalg.{DenseVector, norm}

import scala.io.Source
import scala.reflect._
import smile.mds.MDS

import scala.collection.immutable.Range.inclusive

case class TSNP(snpIdx: Long, snpId: String, snpPos: Int, snp: Vector[Int]){
  def this(snpIdx: Long, snpId: String, snpPos: Int) = this(snpIdx, snpId, snpPos, null)
}

case class Parameters(
                     chrFile : String,
                     panelFile : String,
                     missingRate : Double,
                     isFrequencies : Boolean,
                     var infFreq : Double,
                     var supFreq : Double,
                     isLDPruning : Boolean,
                     ldMethod : String,
                     ldTreshold : Double,
                     ldMaxBp : Int,
                     ldMaxN : Int,
                     isDimRed : Boolean,
                     isPCA : Boolean,
                     pcaMethod : String,
                     isMDS : Boolean,
                     mdsMethod : String
                     ){
  def this() = this("file:///home/anastasiia/1000genomes/ALL.chrMT.phase3_callmom-v0_4.20130502.genotypes.vcf.adam",
    "file:///home/anastasiia/1000genomes/ALL.panel",
    0., true, 0.005, 1., true, "compsite", 0.2, 500000, Int.MaxValue, true, true, "GramSVD", false, null)
}

object Clustering{
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PopStrat").setMaster("local").set("spark.ext.h2o.repl.enabled", "false")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    var parametersFile : String = null
    var parameters : Parameters = null

    if (args.length == 0) {
      parameters = new Parameters()
    }
    else if (args.length == 1) {
      parametersFile = args(0)

      val params = sc.textFile(parametersFile).map(line => line.split(",").map(elem => elem.trim))
      val keys = params.map(x => x(0)).collect
      val values = params.map(x => x(1)).collect
      val paramsMap = keys.zip(values).toMap

      if (paramsMap.get("missing_rate").getOrElse(null).toDouble < 0. || paramsMap.get("inf_freq").getOrElse(null).toDouble < 0. || paramsMap.get("sup_freq").getOrElse(null).toDouble < 0.
      || paramsMap.get("ld_treshold").getOrElse(null).toDouble < 0. || paramsMap.get("ld_max_bp").getOrElse(null).toInt < 0 || paramsMap.get("ld_max_n").getOrElse(null).toInt < 0){
        println ("Some of parameters are negative")
        sys.exit(-1)
      }

      if (paramsMap.get("missing_rate").getOrElse(null).toDouble > 1. || paramsMap.get("ld_treshold").getOrElse(null).toDouble > 1.){
        println ("Missing rate or ld treshold is great than 1")
        sys.exit(-1)
      }

      val ldMethods = Set("composite", "corr", "r", "dprime")
      if (ldMethods.contains(paramsMap.get("ld_method").getOrElse(null)) == false){
        println ("There is no such method for calculating the linkage disequilibrium")
        sys.exit(-1)
      }

      val pcaMethods = Set("GramSVD", "GLRM", "Power", "Randomized")
      if (pcaMethods.contains(paramsMap.get("pca_method").getOrElse(null)) == false){
        println ("There is no such method for PCA")
        sys.exit(-1)
      }

      parameters = new Parameters(
        paramsMap.get("chr_file").getOrElse(null), paramsMap.get("panel_file").getOrElse(null),
        paramsMap.get("missing_rate").getOrElse(null).toDouble, paramsMap.get("frequencies").getOrElse(null).toBoolean,
        paramsMap.get("inf_freq").getOrElse(null).toDouble, paramsMap.get("sup_freq").getOrElse(null).toDouble,
        paramsMap.get("ld_pruning").getOrElse(null).toBoolean, paramsMap.get("ld_method").getOrElse(null),
        paramsMap.get("ld_treshold").getOrElse(null).toDouble, paramsMap.get("ld_max_bp").getOrElse(null).toInt,
        paramsMap.get("ld_max_n").getOrElse(null).toInt, paramsMap.get("dim_reduction").getOrElse(null).toBoolean,
        paramsMap.get("pca").getOrElse(null).toBoolean, paramsMap.get("pca_method").getOrElse(null),
        paramsMap.get("mds").getOrElse(null).toBoolean, paramsMap.get("mds_method").getOrElse(null)
      )

      if (parameters.isLDPruning == true) {
        parameters.infFreq = 0.
        parameters.supFreq = 0.
      }
    }

    implicit class RDDOps[T](rdd: RDD[Seq[T]]) {
      def transpose(): RDD[Seq[T]] = {
        rdd.zipWithIndex.flatMap {
          case (row, rowIndex) => row.zipWithIndex.map {
            case (number, columnIndex) => columnIndex -> (rowIndex, number)
          }
        }.groupByKey.sortByKey().values.map {
          indexedRow => indexedRow.toSeq.sortBy(_._1).map(_._2)
        }
      }
    }

/*        val gtsForSample: RDD[Genotype] = sc.loadGenotypes(genotypeFile)
        val start = 16050000
        val end   = 16053000
        val sampledGts = gtsForSample.filter(g => (g.getVariant.getStart >= start && g.getVariant.getEnd <= end) )
        sampledGts.adamParquetSave("/home/anastasiia/1000genomes/chr22-sample_3000.adam")*/

    val populations = Array("AFR", "EUR", "AMR", "EAS", "SAS")

    def extract(file: String, superPop: String = "pop", filter: (String, String) => Boolean): Map[String, String] = {
      Source.fromFile(file).getLines().map(line => {
        val tokens = line.split("\t").toList
        if (superPop == "pop") {
          tokens(0) -> tokens(1)
        } else {
          tokens(0) -> tokens(2)
        }
      }
      ).toMap.filter(tuple => filter(tuple._1, tuple._2))
    }

    val panel : Map[String, String] = extract(parameters.panelFile, "super_pop", (sampleID: String, pop: String) => populations.contains(pop))
    val bpanel = sc.broadcast(panel)
    val allGenotypes: RDD[Genotype] = sc.loadGenotypes(parameters.chrFile)
    val genotypes: RDD[Genotype] = allGenotypes.filter(genotype => {
      bpanel.value.contains(genotype.getSampleId)
    })

    val gts = new Population(sc, sqlContext, genotypes, panel, parameters.missingRate, parameters.infFreq, parameters.supFreq)
    val variantsRDD: RDD[(String, Array[SampleVariant])] = gts.sortedVariantsBySampelId
    println(s"Number of SNPs is ", variantsRDD.first()._2.length)

    /* LDPruning */

    var prunnedSnpIdSet: List[String] = null

    if (parameters.isLDPruning == true) {
      val ldSize = 256
      val seq = sc.parallelize(0 until ldSize * ldSize)
      val ldPrun = new LDPruning(sc, sqlContext, seq)

      def toTSNP(snpId: String, snpPos: Long, snp: Vector[Int]): TSNP = {
        new TSNP(snpPos, snpId, snpId.split(":")(1).toInt, snp)
      }

      val snpIdSet: RDD[(String, Long)] = sc.parallelize(variantsRDD.first()._2.map(_.variantId)).zipWithIndex()

      def transposeRDD[T](rdd: RDD[Seq[T]]): RDD[Seq[T]] = {
        rdd.zipWithIndex.flatMap {
          case (row, rowIndex) => row.zipWithIndex.map {
            case (number, columnIndex) => columnIndex -> (rowIndex, number)
          }
        }.groupByKey.sortByKey().values.map {
          indexedRow => indexedRow.toSeq.sortBy(_._1).map(_._2)
        }
      }

      var snpSet: RDD[Seq[Int]] = variantsRDD.map { case (_, sortedVariants) => sortedVariants.map(_.alternateCount.toInt) }
      snpSet = transposeRDD(snpSet)

      def getListGeno(iditer: Iterator[(String, Long)], snpiter: Iterator[Seq[Int]]): Iterator[TSNP] = {
        var res = List[TSNP]()
        while (iditer.hasNext && snpiter.hasNext) {
          val iditerCur = iditer.next
          val tsnp = toTSNP(iditerCur._1, iditerCur._2, snpiter.next.toVector)
          res ::= tsnp
        }
        res.iterator
      }

      val listGeno = snpIdSet.repartition(1).zipPartitions(snpSet.repartition(1))(getListGeno)
      // val listGeno = snpIdSet.zip(snpSet).map{case((snpId, snpPos), snp) => toTSNP(snpId, snpPos, snp.toVector)}
      // println(listGeno.collect().toList)

      prunnedSnpIdSet = ldPrun.performLDPruning(listGeno, parameters.ldMethod, parameters.ldTreshold, parameters.ldMaxBp, parameters.ldMaxN)
      println(prunnedSnpIdSet)
    }

    /*LDPruning end*/

    var ds = gts.getDataSet(variantsRDD, prunnedSnpIdSet)
    println(ds.count(), ds.columns.length)

    /* outliers */

    var samplesForOutliers: RDD[Seq[Int]] = variantsRDD.map {
      case (_, sortedVariants) =>
        sortedVariants.filter(varinat => prunnedSnpIdSet contains(varinat.variantId)).map(_.alternateCount.toInt)
    }

    val n: Int = samplesForOutliers.count().toInt
    val randomindx : RDD[List[Int]]         = sc.parallelize((1 to n/2).map(unused => Random.shuffle(0 to n - 1).take(n/2).toList))
    val rddWithIdx : RDD[(Seq[Int], Long)]  = samplesForOutliers.zipWithIndex()
    val subRDDs : RDD[Array[Seq[Int]]] = randomindx.map(idx => rddWithIdx.filter{case(_, sampleidx) => idx.contains(sampleidx)}.map(_._1).collect)

/*    def computeMeanVar(iter: Iterator[Array[Seq[Int]]]) : Iterator[Array[(Double, Double)]] = {
      var meanVar: List[Array[(Double, Double)]] = List()
      while(iter.hasNext){
        val iterCurr = iter.next
        val meanstddev = iterCurr.map{row =>
          val mean = row.sum / n.toDouble
          val variance = row.map(score => (score - mean) * (score - mean))
          val stddev = Math.sqrt(variance.sum / (n.toDouble - 1))
          (mean, stddev)
        }
        meanVar ::= meanstddev
      }
      meanVar.iterator
    }

    val meanVar : RDD[Array[(Double, Double)]] = subRDDs.mapPartitions(computeMeanVar)*/

    val meanVar : RDD[Array[(Double, Double)]] = subRDDs.map { currRDD =>
      val meanStd : Array[(Double, Double)]= currRDD.map { row =>
        val mean = row.sum / n.toDouble
        val variance = row.map(score => (score - mean) * (score - mean))
        val stddev = Math.sqrt(variance.sum / (n.toDouble - 1))
        (mean, stddev)
      }
      meanStd
    }

    meanVar.foreach(x => println(x.toList))

    // println(subRDDs.length, subRDDs(0).count)
    /* outliers end*/







/* MDS-------------------------------------------------------------------------------------------------------------

    /* mds */ // sql ds is input for

    var snpMDS: RDD[Seq[Int]] = variantsRDD.map {
      case (_, sortedVariants) =>
        sortedVariants.filter(varinat => prunnedSnpIdSet contains(varinat.variantId)).map(_.alternateCount.toInt)
    }
    snpMDS = transposeRDD(snpMDS)



    val mds = new MDSReduction(sc, sqlContext)
    val pc: RDD[Array[Double]]= sc.parallelize(mds.computeMDS(snpMDS, "classic"))
    println("mds: ")
    println(pc.count, pc.first.length)

    val samplesSet: RDD[String] = variantsRDD.map(_._1)
    val mdsRDD: RDD[(String, Array[Double])] = samplesSet.zip(pc)

    ds = gts.getDataSet(mdsRDD)
    println(ds.count(), ds.columns.length)
    ds.show(50)

    /* mds end*/

 MDS- ----------------------------------------------------------------------------------------------------------- */

























        /*var prunnedVariants : RDD[Array[SampleVariant]] = variantsRDD.values
        if (prunnedSnpIdSet.length > 0 )
          prunnedVariants = variantsRDD.values.map(variant => variant.filter(prunnedSnpIdSet contains _.variantId))
    */

        //    val ds = gts.getDataSet(variantsRDD, prunnedSnpIdSet)
    //    println(ds.count(), ds.columns.length)


    ////    val snpIdSet: List[String] = ldPrun.performLDPruning(variantsRDD, "composite", 0.2)
    ////    val ds = gts.getDataSet(variantsRDD, snpIdSet)
    //
    //    var ds = gts.getDataSet(variantsRDD)
    //    println(ds.count(), ds.columns.length)
    //
    //    val snps = ds.drop("SampleId").drop("Region")
    //    val rddSnps : RDD[Vector] = snps.rdd.map(row => Vectors.dense(row.toSeq.toArray.map(x => x.asInstanceOf[Double])))
    //
    //    val rddArraySnps : RDD[Array[Double]] =  snps.rdd.map(row => row.toSeq.toArray.map(x => x.asInstanceOf[Double]))
    //
    //    import smile.math.distance{EuclideanDistance}
    //
    //
    //    val distEuclid =
    //
    //    def computeEuclidean(v1: Vector, v2: Vector): Double = {
    //      val b1 = new DenseVector(v1.toArray)
    //      val b2 = new DenseVector(v2.toArray)
    //      norm((b1 - b2), 2.0)
    //    }
    //
    //    val matr: Array[Double] = rddSnps.cartesian(rddSnps).map{case(vec1, vec2) => computeEuclidean(vec1, vec2)}.collect()
    //    val cartesMatr = matr.grouped(math.sqrt(matr.length).toInt).toArray.map(_.toArray)
    //    println(cartesMatr.length)
    //
    //    def mds(proximity: Array[Array[Double]], k: Int, add: Boolean = false): MDS = new MDS(proximity, k, add)
    //
    //    val coord = mds(cartesMatr, 2).getCoordinates.toList.map(x => x.toList)
    //
    //
    //    val v1 = Vectors.dense(1.0, 2.0, 3.0)
    //    val v2 = Vectors.dense(4.0, 5.0, 6.0)
    //    val bv1 = new DenseVector(v1.toArray)
    //    val bv2 = new DenseVector(v2.toArray)
    //    val vectout = Vectors.dense((bv1 + bv2).toArray)
    //    vectout.toArray.toList//
    //    // def mds(proximity: Array[Array[Double]], k: Int, add: Boolean = false): MDS = new MDS(proximity, k, add)
    //    val matrTest = Array(Array(0.0, 2.0, 3.4),
    //      Array(3.0, 0.0, 4.5),
    //      Array(1.2, 3.4, 0.0))
    //    val variance = mds(matrTest, 2).getProportion.toList
    //    val coordTest = mds(matrTest, 2).getCoordinates.toList.map(x => x.toList)
    //
    //    print(coordTest)*/



/*    val byColumnAndRow = rddSnps.zipWithIndex.flatMap {
      case (row, rowIndex) => row.zipWithIndex.map {
        case (number, columnIndex) => columnIndex -> (rowIndex, number)
      }
    }
    // Build up the transposed matrix. Group and sort by column index first.
    val byColumn = byColumnAndRow.groupByKey.sortByKey().values
    // Then sort by row index.
    val transposed : RDD[Vector] = byColumn.map {
      indexedRow => Vectors.dense(indexedRow.toArray.sortBy(_._1).map(_._2).map(x => x.toDouble))
    }

    val similarMat = new RowMatrix(transposed).columnSimilarities()

    similarMat.entries.first()*/


   // val transposedMatrix : Array[Array[Double]] = transposed.collect.map(row => row.toArray)



    //    val assembler = new VectorAssembler()
//      .setInputCols(ds.drop("SampleId").drop("Region").columns)
//      .setOutputCol("features")
//
//    var df = assembler.transform(ds)
//    val asDense = udf((v: Vector) => v.toDense)
//    val vecToSeq = udf((v: Vector) => v.toArray)
//
//    df = df.withColumn("features", asDense($"features"))
//
//    val scaler = new StandardScaler()
//      .setInputCol("features")
//      .setOutputCol("scaledFeatures")
//      .setWithStd(true)
//      .setWithMean(true)
//
//    var transformedDF =  scaler.fit(df).transform(df)
//
//    val snpMatr : RDD[Vector] = transformedDF.select("scaledFeatures").rdd.map(row => Vectors.dense(row.toSeq.toArray.map(x => x.asInstanceOf[Double])))
//    val mat = new RowMatrix(snpMatr).columnSimilarities().toIndexedRowMatrix()






    // val coord = mds(snpMatr, 2).getCoordinates.toList.map(x => x.toList)


 //   transformedDF.select("scaledFeatures")show(10)

//    val pcaDS = unsupervisedModel.pcaH2O(ds)
//    val kmeansPred = unsupervisedModel.kMeansH2OTrain(pcaDS, populations.size)
//    kmeansPred.foreach(println)
//    unsupervisedModel.writecsv(kmeansPred, "/home/anastasiia/IdeaProjects/kmeanspca_super_pop_chr22.csv")



    //def mds(proximity: Array[Array[Double]], k: Int, add: Boolean = false): MDS = new MDS(proximity, k, add)


    // Subpopulations

/*
    val clusters = inclusive(0, populations.length - 1)
    val sampleIdInCluster = clusters.map(cluster => kmeansPred.filter(kmeansPred("Predict") === cluster).select("SampleId", "Region", "Predict"))
    val sampleIdSet = clusters.map(cluster => kmeansPred.filter(kmeansPred("Predict") === cluster).select("SampleId").collect().map(row => row.mkString))

/*    sampleIdSet.foreach(ds => {
      println("New one")
      ds.foreach(println)
    }
    )*/

    val panelsSubPopulation = sampleIdSet.map(samples => extract(panelFile, "pop", (sampleID: String, pop: String) => samples.contains(sampleID)))
    val bpanelsSubPopulation = panelsSubPopulation.map(panel => sc.broadcast(panel))
    val genotypesSubPop = bpanelsSubPopulation.map(panelSubPopulation => genotypes.filter(genotype => panelSubPopulation.value.contains(genotype.getSampleId)))

    val subGts = (genotypesSubPop, panelsSubPopulation).zipped.map{(subPop, panelSubPop) => {
      var supFreq = 1.0
      var newPop = new Population(sc, sqlContext, subPop, panelSubPop, 0.05, supFreq)
      val snpIdSet = ldPrun.performLDPruning(newPop.sortedVariantsBySampelId, "composite", 0.5)
      val dataSet = newPop.getDataSet(newPop.sortedVariantsBySampelId, snpIdSet)
      /*      while (dataSet.columns.length == 2){
              supFreq += 0.001
              newPop = new Population(sc, sqlContext, subPop, panelSubPop, 0.001, supFreq)
            }*/
      (newPop, dataSet)}
    }

    val kmeansPredictions = subGts.map { case (pop, ds) =>
      println("\nNew SubPopulation: \n")
      // println(genotype.dataSet.foreach(println))
      val pcaDS = unsupervisedModel.pcaH2O(ds)
      unsupervisedModel.kMeansH2OTrain(pcaDS, pop._numberOfRegions)
    }
    (clusters, kmeansPredictions).zipped.map{(cluster, prediction) => unsupervisedModel.writecsv(prediction, s"/home/anastasiia/IdeaProjects/chr22/kmeanspca_$cluster.csv")}
*/




/*    val snp1 = Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0)
    val snp2 = Vector(0, 2, 0, 2, 2, 1, 1, 1, 0, 0, 2, 0, 2, 2, 1, 1, 1, 3)
    println("cor :")
    println("composite: ",  ldPrun.pairComposite(snp1, snp2))
    println("corr: ",  ldPrun.pairCorr(snp1, snp2))
    println("r: ",  ldPrun.pairR(snp1, snp2))
    println("dprime: ",  ldPrun.pairDPrime(snp1, snp2))*/





    // println(ds.columns.length, ds.count())
    // val prunnedDs = gts.performLDPruning(ds, slideMaxN = 100)
    // println(prunnedDs.columns.length, prunnedDs.count())

    // println(ds.select(snpsetId(2)).rdd.collect().map(_.getInt(0)).toVector)
    //println((ds(snpsetId(3)).toString()))

    /*    val corr = gts.pairComposite(ds.select(snpsetId(2)).rdd.collect().map(_.getInt(0)).toVector,ds.select(snpsetId(3)).rdd.collect().map(_.getInt(0)).toVector)

        var s = ds.select("SampleId", snpsetId(3))
        println(s.columns.length)
        s = s.join(ds.select("SampleId", snpsetId(4)), "SampleId")
        println(s.columns.length, s.count())
        println(s.columns)
        println(corr)*/

    /*
    val pcaDS = unsupervisedModel.pcaH2O(gts.dataSet)
    println(pcaDS._key.get())
    val kmeansPred = unsupervisedModel.kMeansH2OTrain(pcaDS, populations.size)
    kmeansPred.foreach(println)
    unsupervisedModel.writecsv(kmeansPred, "/home/anastasiia/IdeaProjects/chr22/kmeanspca_super_pop.csv")

    /* println ("One more time")
    val pcaDS1 = unsupervisedModel.pcaH2O(Gts.dataSet)
    println(pcaDS1._key.get())
    val kmeandPred1 = unsupervisedModel.kMeansH2OTrain(pcaDS1, 4)
    println(kmeandPred1) */

*/


    /*
    val subGts = new Population(sc, sqlContext, genotypesSubPop(0), panelsSubPopulation(0))
    val pcaDS1 = unsupervisedModel.pcaH2O(subGts.dataSet)
    println(pcaDS1._key.get())
    val kmeansPred1 = unsupervisedModel.kMeansH2OTrain(pcaDS1, 11)
    kmeansPred1.foreach(println)
    unsupervisedModel.writecsv(kmeansPred1, "/home/anastasiia/IdeaProjects/kmeanspca.csv")
*/
    /*
        val kmeansPredSetMethods = pcaMethods.map(method => {
          val pcaDS = unsupervisedModel.pcaH2O(Gts.dataSet, pcaMethod = method)
          unsupervisedModel.kMeansH2OTrain(pcaDS, 3)
        })
    */

    // unsupervisedModel.writecsv(kmeansPredSetMethods.toList(1), "/home/anastasiia/IdeaProjects/kmeanspca.csv")
    /*
        kmeansPredSetMethods.map(pred => {
          println("New DS: \n")
          pred.foreach(println)
        })*/



    //unsupervisedModel += Gts
    //unsupervisedModel += new Population(sc, sqlContext, genotypes, panel)

    // unsupervisedModel.members.map(memeber => println(memeber.dataSet))

    //unsupervisedModel.members.map(member => println("New frame:\n" + member.pcaDs._key.get()))

    // val sortedVariantsBySampleId = unsupervisedModel.members(0).getVariantsBySampelId( .3, .5)

    // Create the SchemaRDD from the header and rows and convert the SchemaRDD into a H2O dataframe

    /*val pops = unsupervisedModel.members
    pops.map(pop => pop.schemaRDD.collect().map(println))
*/


    //print(unsupervisedModel.h2oFrame)
    /*import schemaRDD.sqlContext.implicits._

    val dataFrame = h2oContext.asH2OFrame(schemaRDD)
    println("Git 6")
    dataFrame.replace(dataFrame.find("Region"), dataFrame.vec("Region").toCategoricalVec()).remove()
    dataFrame.update()

    println(dataFrame._key)
    dataFrame.vecs(Array.range(3, dataFrame.numCols())).map(i => i.toNumericVec)*/

    println("Git")

  }
}

