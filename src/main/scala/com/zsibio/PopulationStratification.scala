package com.zsibio

/**
  * Created by anastasiia on 10/6/16.
  */

import java.io.File

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, RandomForestClassificationModel}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.sysml.api.ml.SVMModel
import smile.classification.RandomForest

import scala.util.Random
import scala.io.Source
import scala.collection.JavaConverters._

case class TSNP(snpIdx: Long, snpId: String, snpPos: Int, snp: Vector[Int]){
  def this(snpIdx: Long, snpId: String, snpPos: Int) = this(snpIdx, snpId, snpPos, null)
}

case class ClassificationMetrics(error: Double, precisionByLabels: List[Double], recallByLabels: List[Double], fScoreByLabels: List[Double])

class Parameters(
                  chrFile : String,
                  panelFile : String,
                  pop : String,
                  popSet : Array[String],
                  missingRate : Double,
                  isFrequencies : Boolean,
                  infFreq : Double,
                  supFreq : Double,
                  isLDPruning : Boolean,
                  ldMethod : String,
                  ldTreshold : Double,
                  ldMaxBp : Int,
                  ldMaxN : Int,
                  isOutliers : Boolean,
                  isDimRed : Boolean,
                  dimRedMethod : String,
                  pcaMethod : String,
                  mdsMethod : String,
                  isClustering : Boolean,
                  isClassification : Boolean,
                  clusterMethod : String,
                  classificationMethod : String,
                  cvClassification : Boolean,
                  cvClustering : Boolean,
                  nRepeatClassification : Int,
                  nRepeatClustering : Int,
                  chrFreqFile : String,
                  chrFreqFileOutput : String
                ){
  def this() = this("file:///home/anastasiia/1000genomes/ALL.chrMT.phase3_callmom-v0_4.20130502.genotypes.vcf.adam",
    "file:///home/anastasiia/1000genomes/ALL.panel", "super_pop", Array("AFR", "EUR", "AMR", "EAS", "SAS"),
    0., true, 0.005, 1., true, "compsite", 0.2, 500000, Int.MaxValue, false, true, "PCA", "GramSVD", null,
    true, false, "svm", null, true, false, 1, 1, null, "variants_frequencies.csv")

  val _chrFile = chrFile
  val _panelFile = panelFile
  val _pop = pop
  val _popSet = popSet
  val _missingRate = missingRate
  val _isFrequencies : Boolean = isFrequencies
  var _infFreq : Double = infFreq
  var _supFreq : Double = supFreq
  val _isLDPruning : Boolean = isLDPruning
  val _ldMethod : String = ldMethod
  val _ldTreshold : Double = ldTreshold
  val _ldMaxBp : Int = ldMaxBp
  val _ldMaxN : Int = ldMaxN
  val _isOutliers : Boolean = isOutliers
  val _isDimRed : Boolean = isDimRed
  val _dimRedMethod : String =dimRedMethod
  val _pcaMethod : String = pcaMethod
  val _mdsMethod : String = mdsMethod
  val _isClustering : Boolean = isClustering
  val _isClassification : Boolean = isClassification
  val _clusterMethod : String = clusterMethod
  val _classificationMethod : String = classificationMethod
  val _cvClassification : Boolean = cvClassification
  val _cvClustering : Boolean = cvClustering
  val _nRepeatClassification : Int = nRepeatClassification
  val _nRepeatClustering : Int = nRepeatClustering
  val _chrFreqFile : String = chrFreqFile
  val _chrFreqFileOutput : String = chrFreqFileOutput
}

object PopulationStratification {

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

  implicit class RDDwr[T, Y, Z](rdd: RDD[(T, Y, Z)]) {
    def writeToCsv(filename: String): Unit = {
      val tmpParquetDir = "hdfs:///popgen/Posts.tmp.parquet"
      rdd.repartition(1).map { case (id, valueY, valueZ) => Array(id, valueY, valueZ).mkString(",")}
        .saveAsTextFile(tmpParquetDir)
      val dir = new File(tmpParquetDir)

      val tmpTsvFile = tmpParquetDir + File.separatorChar + "part-00000"
      (new File(tmpTsvFile)).renameTo(new File(filename))
      dir.listFiles.foreach( f => f.delete )
      dir.delete
    }
  }


  implicit class dfWriter(dataSet: DataFrame) {
    def writeToCsv(filename: String, isHeader: String = "true"): Unit = {
      val tmpParquetDir = "hdfs:///popgen/Posts.tmp.parquet"
      dataSet.write
        .format("com.databricks.spark.csv")
        .option("header", isHeader)
        .save(filename)//tmpParquetDir)

      /*
      val vcf = "hdfs:///popgen/chr22.vcf"
      val gts: RDD[Genotype] = sc.loadGenotypes(vcf)
      val adamFile = "hdfs:///popgen/chr22.adam"
      gts.adamParquetSave(adamFile) */


      /*  val dir = new File(tmpParquetDir)
        val tmpTsvFile = tmpParquetDir + File.separatorChar + "part-00000"
        (new File(tmpTsvFile)).renameTo(new File(filename))
        dir.listFiles.foreach( f => f.delete )
        dir.delete */
    }
  }

  implicit class RDDPart[T](rdd: RDD[T]) {
    def partitionBy(f: T => Boolean): (RDD[T], RDD[T]) = {
      val passes = rdd.filter(f)
      val fails = rdd.filter(e => !f(e))
      (passes, fails)
    }
  }

  def computeAvgMetrics (classificationMetrics: List[ClassificationMetrics], isPrint: Boolean = true) : Unit ={
    val avgError = classificationMetrics.map(metrics => metrics.error).sum / classificationMetrics.size
    val avgPrecisionByLabel = classificationMetrics.map(metrics => metrics.precisionByLabels).transpose.map(precisions => precisions.sum / precisions.size)
    val avgRecallByLabel = classificationMetrics.map(metrics => metrics.recallByLabels).transpose.map(recalls => recalls.sum / recalls.size)
    val avgFScoreByLabel = classificationMetrics.map(metrics => metrics.fScoreByLabels).transpose.map(fScores => fScores.sum / fScores.size)

    if(isPrint){
      println(s"Average error: $avgError")
      println(s"Average precision for each label:"); avgPrecisionByLabel.foreach{precision => print(s" $precision")}
      println(s"\nAverage recall for each label:"); avgRecallByLabel.foreach(recall => print(s" $recall"))
      println(s"\nAverage F-Measure for each label:"); avgFScoreByLabel.foreach(f => print(s" $f"))
    }
  }

  def variantId(genotype: Genotype): String = {
    val name = genotype.getVariant.getContig.getContigName
    val start = genotype.getVariant.getStart
    val end = genotype.getVariant.getEnd
    s"$name:$start:$end"
  }

  def altCount(genotype: Genotype): Int = {
    genotype.getAlleles.asScala.count(_ != GenotypeAllele.Ref)
  }

  def prun(rdd: RDD[(String, Array[(String, Int)])], snpIdSet: List[String]): RDD[(String, Array[(String, Int)])] =
    rdd.map{ case (sample, sortedVariants) => (sample, sortedVariants.filter(varinat => snpIdSet contains (varinat._1)))}

/*
  def prun(rdd: RDD[(String, Array[(String, Int)])], snpIdSet: List[String]): RDD[(String, Array[(String, Int)])] =
    rdd.filter{case(variant, _) => snpIdSet contains variant}
*/

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("PopStrat")
      .set("spark.ext.h2o.repl.enabled", "false")
      //.set("spark.ext.h2o.topology.change.listener.enabled", "false")
      .set("spark.driver.maxResultSize", "0")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val time = new Time()
    var (t0, t1) : (Long, Long) = (0, 0)
    var ds : DataFrame = null
    var trainingDS : DataFrame = null
    var testDS : DataFrame = null

    var parametersFile : String = null
    var parameters : Parameters = null
    var outputFilename : String = null
    var timeResult : List[String] = Nil
    var variantsDF : DataFrame = null
    var nCores: Int = 5

    if (args.length == 0) {
      parameters = new Parameters()
    }
    else if (args.length >= 1) {
      if (args.length == 2) nCores = args(1).toInt

      parametersFile = args(0)

      val params = sc.textFile(parametersFile).map(line => line.split(",").map(elem => elem.trim))
      val keys = params.map(x => x(0)).collect
      val values = params.map(x => x(1)).collect
      val paramsMap = keys.zip(values).toMap

      outputFilename = paramsMap.get("outputFilename").getOrElse(null)

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
        paramsMap.get("population").getOrElse(null), paramsMap.get("population_set").getOrElse(null).split("/"),
        paramsMap.get("missing_rate").getOrElse(null).toDouble, paramsMap.get("frequencies").getOrElse(null).toBoolean,
        paramsMap.get("inf_freq").getOrElse(null).toDouble, paramsMap.get("sup_freq").getOrElse(null).toDouble,
        paramsMap.get("ld_pruning").getOrElse(null).toBoolean, paramsMap.get("ld_method").getOrElse(null),
        paramsMap.get("ld_treshold").getOrElse(null).toDouble, paramsMap.get("ld_max_bp").getOrElse(null).toInt,
        paramsMap.get("ld_max_n").getOrElse(null).toInt, paramsMap.get("outliers").getOrElse(null).toBoolean,
        paramsMap.get("dim_reduction").getOrElse(null).toBoolean, paramsMap.get("dim_red_method").getOrElse(null),
        paramsMap.get("pca_method").getOrElse(null), paramsMap.get("mds_method").getOrElse(null),
        paramsMap.get("clustering").getOrElse(null).toBoolean, paramsMap.get("classification").getOrElse(null).toBoolean,
        paramsMap.get("clustering_method").getOrElse(null), paramsMap.get("classification_method").getOrElse(null),
        paramsMap.get("cv_classification").getOrElse(null).toBoolean, paramsMap.get("cv_clustering").getOrElse(null).toBoolean,
        paramsMap.get("n_reapeat_classification").getOrElse(null).toInt, paramsMap.get("n_reapeat_clustering").getOrElse(null).toInt,
        paramsMap.get("chr_frequencies_file").getOrElse(null),
        paramsMap.get("chr_frequencies_file_output").getOrElse(null)
      )

      if (parameters._isFrequencies == false) {
        parameters._infFreq = 0.
        parameters._supFreq = 1.
      }
    }

    /*        val gtsForSample: RDD[Genotype] = sc.loadGenotypes(genotypeFile)
            val start = 16050000
            val end   = 16053000
            val sampledGts = gtsForSample.filter(g => (g.getVariant.getStart >= start && g.getVariant.getEnd <= end) )
            sampledGts.adamParquetSave("/home/anastasiia/1000genomes/chr22-sample_3000.adam")*/

    def extract(file: String, superPop: String = "super_pop", filter: (String, String) => Boolean) = {
      sc.textFile(file).map(line => {
        val tokens = line.split("\t").toList
        if (superPop == "pop") {
          tokens(0) -> tokens(1)
        } else {
          tokens(0) -> tokens(2)
        }
      }
      ).collectAsMap.filter(tuple => filter(tuple._1, tuple._2))
    }


    val panel = extract(parameters._panelFile, parameters._pop, (sampleID: String, pop: String) => parameters._popSet.contains(pop))
    val bpanel = sc.broadcast(panel)

    val allGenotypes: RDD[Genotype] = sc.loadGenotypes(parameters._chrFile)

    val genotypes: RDD[Genotype] = allGenotypes.filter(genotype => {
      bpanel.value.contains(genotype.getSampleId)
    })

    val df = sqlContext.read.parquet(parameters._chrFile)
    df.registerTempTable("gts")

    if (parameters._chrFreqFile == "null") {
      val variantsNonmissingsDF = sqlContext.sql(
        "select " +
          "concat(variant.contig.contigName, ':', variant.start) as variantId," +
          /*          "variant.contig.contigName," +
                    "variant.start," +
                    "variant.referenceAllele," +
                    "variant.alternateAllele," +*/
          "count(*) as nonmissing " +
          "from gts " +
          "group by variant.contig.contigName, variant.start")

      val nonMultiallelicDF = sqlContext.sql(
        "select " +
          "concat(variant.contig.contigName, ':', variant.start) as variantId " +
          "from gts " +
          "group by sampleId, variant.contig.contigName, variant.start, variant.referenceAllele having count(*)<=1").distinct()

      val variantsFreqDF = sqlContext.sql(
        "select " +
          "concat(variant.contig.contigName, ':', variant.start) as variantId," +
          "variant.contig.contigName," +
          "variant.start," +
          "variant.referenceAllele," +
          "variant.alternateAllele," +
          "sum(case when alleles[0] = alleles[1] then 2 else 1 end) as frequency " +
          "from gts " +
          "where alleles[0] = 'Alt' or alleles[1] = 'Alt' " +
          "group by variant.contig.contigName, variant.start, variant.`end`, variant.referenceAllele, variant.alternateAllele")

      variantsDF = variantsNonmissingsDF.join(nonMultiallelicDF, "variantId").join(variantsFreqDF, "variantId")

      variantsDF.repartition(1).writeToCsv(parameters._chrFreqFileOutput)

    }
    else
      variantsDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(parameters._chrFreqFile)

    variantsDF.registerTempTable("variants")

    var genotypesDF = sqlContext.sql(
      "select " +
        "sampleId," +
        "concat(variant.contig.contigName, ':', variant.start) as variantId, " +
        /* "variant.contig.contigName,"+
           "variant.start,"+
           "variant.referenceAllele," +
           "variant.alternateAllele," +*/
        "case when alleles[0] = 'Alt' and alleles[1] = 'Alt' then 2 when alleles[0] = 'Ref' and alleles[1] = 'Ref' then 0 else 1 end as count " +
        "from gts " +
        "group by sampleId, variant.contig.contigName, variant.start, alleles").distinct()

    val sampleCount = sqlContext.sql(
      "select sampleId from gts group by sampleId")
      .count

    println(s"Sample number: $sampleCount")
    genotypesDF.show(10)

    val filteredVariantsDF = sqlContext.sql(
      "select variantId from variants " +
        s"where nonmissing >= $sampleCount * (1 - ${parameters._missingRate}) and " +
        s"frequency >= ${parameters._infFreq} * $sampleCount and frequency <= ${parameters._supFreq} * $sampleCount"
    )

    val bFilteredVariantsDF = broadcast(filteredVariantsDF.as("filteredVariants"))
    val gtsDF = genotypesDF.as("df").join(broadcast(bFilteredVariantsDF), "variantId").distinct

    // val filteredVariantsList : Array[Any] = filteredVariantsDF.select("VariantId").map(_.get(0)).collect
    // variantsDF = variantsDF.groupBy("sampleId").pivot("variantId", filteredVariantsList.toSeq).agg(expr("first(count)").cast("int"))

    t0 = System.currentTimeMillis()
    gtsDF.show(30)
    t1 = System.currentTimeMillis()
    val fsFreqTime = time.formatTimeDiff(t0, t1)
    println(s"Feature selection. Frequencies: $fsFreqTime")
    timeResult ::= (s"Feature selection. Frequencies: $fsFreqTime")

    //gtsDF.repartition(1).writeToCsv("/home/anastasiia/gts.csv")

    val sampleToData : RDD[(String, (String, Int))]= gtsDF.rdd.map({case Row(variantId : String, sampleId: String, count : Int) => (sampleId, (variantId, count))})//=> (row(0).toString, (row(1).toString, row(2).cast[Int])//asInstanceOf[Int])))
    val groupedSampleData : RDD[(String, Iterable[(String, Int)])] = sampleToData.groupByKey()
    var variantsRDD : RDD[(String, Array[(String, Int)])] = groupedSampleData.mapValues(it => it.toArray.sortBy(_._1)).cache()


    t0 = System.currentTimeMillis()
    println(s"Variants number: ${variantsRDD.count}")
    t1 = System.currentTimeMillis()
    val dfRDDtransformTime = time.formatTimeDiff(t0, t1)
    println(s"DF -> RDD: $dfRDDtransformTime")
    timeResult ::= (s"DF -> RDD: $dfRDDtransformTime")

    val gts = new PopulationGe(sc, sqlContext, genotypes, panel, parameters._missingRate, parameters._infFreq, parameters._supFreq)

    /**
      * Variables for prunning data. Used for both ldPruning and Outliers detection cases
      */

    var variantsRDDprunned: RDD[(String, Array[(String, Int)])] = variantsRDD
    var prunnedSnpIdSet: List[String] = null

    /**
      *  LDPruning
      */

    if (parameters._isLDPruning == true) {
      println("LD Pruning")
      t0 = System.currentTimeMillis()

      val ldSize = 256
      val seq = sc.parallelize(0 until ldSize * ldSize)
      val ldPrun = new LDPruning(sc, sqlContext, seq)

      def toTSNP(snpId: String, snpPos: Long, snp: Vector[Int]): TSNP = {
        new TSNP(snpPos, snpId, snpId.split(":")(1).toInt, snp)
      }

      val snpIdSet: RDD[(String, Long)] = sc.parallelize(variantsRDD.first()._2.map(_._1)).zipWithIndex()

      var snpSet: RDD[Seq[Int]] = variantsRDD.map { case (_, sortedVariants) => sortedVariants.map(_._2.toInt) }
      snpSet = snpSet.transpose()

      def getListGeno(iditer: Iterator[(String, Long)], snpiter: Iterator[Seq[Int]]): Iterator[TSNP] = {
        var res = List[TSNP]()
        while (iditer.hasNext && snpiter.hasNext) {
          val iditerCur = iditer.next
          val tsnp = toTSNP(iditerCur._1, iditerCur._2, snpiter.next.toVector)
          res ::= tsnp
        }
        res.iterator
      }

      val nPartitions = 4 * nCores//snpSet.getNumPartitions
      val listGeno = snpIdSet.repartition(nPartitions).zipPartitions(snpSet.repartition(nPartitions))(getListGeno)
      // val listGeno = snpIdSet.zip(snpSet).map{case((snpId, snpPos), snp) => toTSNP(snpId, snpPos, snp.toVector)}
      // println(listGeno.collect().toList)

      val bPrunnedSnpIdSet = sc.broadcast(ldPrun.performLDPruning(listGeno, parameters._ldMethod, parameters._ldTreshold, parameters._ldMaxBp, parameters._ldMaxN))
      println(bPrunnedSnpIdSet.value)
      variantsRDDprunned = prun(variantsRDDprunned, bPrunnedSnpIdSet.value)

      t1 = System.currentTimeMillis()
      val fsLdTime = time.formatTimeDiff(t0, t1)
      println(s"Feature selection. LD Pruning: $fsLdTime")
      timeResult ::= (s"Feature selection. LD Pruning: $fsLdTime")
    }

    /**
      * Outliers detection
      */

    if (parameters._isOutliers == true) {
      println("Outliers Detections")
      val samplesForOutliers: RDD[Seq[Int]] = variantsRDDprunned.map {case (_, sortedVariants) => sortedVariants.map(_._2) }
      val n: Int = samplesForOutliers.count().toInt
      val randomindx: RDD[List[Int]] = sc.parallelize((1 to n / 2).map(unused => Random.shuffle(0 to n - 1).take(n / 2).toList))
      val rddWithIdx: RDD[(Seq[Int], Long)] = samplesForOutliers.zipWithIndex()
      val subRDDs: RDD[Array[Seq[Int]]] = randomindx.map(idx => rddWithIdx.filter { case (_, sampleidx) => idx.contains(sampleidx) }.map(_._1).collect)

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

      val meanVar: RDD[Array[(Double, Double)]] = subRDDs.map { currRDD =>
        val meanStd: Array[(Double, Double)] = currRDD.map { row =>
          val mean = row.sum / n.toDouble
          val variance = row.map(score => (score - mean) * (score - mean))
          val stddev = Math.sqrt(variance.sum / (n.toDouble - 1))
          (mean, stddev)
        }
        meanStd
      }

      meanVar.foreach(x => println(x.toList))

      variantsRDDprunned = prun(variantsRDDprunned, prunnedSnpIdSet)
      // println(subRDDs.length, subRDDs(0).count)
    }

    /**
      * Dimensionality Reduction
      */

    if (parameters._isDimRed == true) {
      println("Dimensionality Reduction")
      // val unsupervised = new Unsupervised(sc, sqlContext)
      t0 = System.currentTimeMillis()

      parameters._dimRedMethod match {

        /**
          * PCA
          */

        case "PCA" =>{
          println(" PCA")
          //  val unsupervised = new Unsupervised(sc, sqlContext)
          val pcaDimRed = new PCADimReduction(sc, sqlContext)
          ds = gts.getDataSet(variantsRDDprunned)
          println(ds.count(), ds.columns.length)
          var nPC = math.min(ds.count, ds.columns.length - 2).toInt
          nPC = if (nPC > 30) 30 else (nPC - 1)
          val variantion = 0.7
          ds = pcaDimRed.pcaML(ds, nPC, "Region", variantion, "tempPcaFeatures")
          pcaDimRed.explainedVariance(ds, nPC, varianceTreshold = variantion, "tempPcaFeatures")
          ds = pcaDimRed.pcaML(ds, pcaDimRed._nPC, "Region", variantion, "pcaFeatures")
          ds.show(10)
          // ds = unsupervised.pcaH2O(ds)
        }
        /**
          * MDS
          */

        case "MDS" => {
          println(" MDS")
          var snpMDS: RDD[Seq[Int]] = variantsRDDprunned.map {case (_, sortedVariants) => sortedVariants.map(_._2.toInt) }
          snpMDS = snpMDS.transpose

          val mds = new MDSReduction(sc, sqlContext)
          val pc: RDD[Array[Double]] = sc.parallelize(mds.computeMDS(snpMDS, "classic"))
          println("mds: ")
          println(pc.count, pc.first.length)

          val samplesSet: RDD[String] = variantsRDD.map(_._1)
          val mdsRDD: RDD[(String, Array[Double])] = samplesSet.zip(pc)

          ds = gts.getDataSet(mdsRDD)
          println(ds.count(), ds.columns.length)
          ds.show(50)
        }
      }
      t1 = System.currentTimeMillis()
      val dimRedTime = time.formatTimeDiff(t0, t1)
      println(s"Dimensionality reduction. ${parameters._dimRedMethod}: $dimRedTime")
      timeResult ::= (s"Dimensionality reduction. ${parameters._dimRedMethod}: $dimRedTime")
    }

    if (ds == null)
      ds = gts.getDataSet(variantsRDDprunned)

    val seeds = 1234
    var split = ds.randomSplit(Array(0.7, 0.3), seeds)
    trainingDS = ds //split(0)
    testDS = split(1)

    /**
      * Clustering
      */

    if (parameters._isClustering == true) {
      println("Clustering")
      var trainPrediction: DataFrame = null
      var testPrediction: DataFrame = null
      val clustering = new Clustering(sc, sqlContext)
      val setK : Seq[Int] = Seq(1, 2, 3, 4, 5)
      var k = parameters._popSet.length
      var avgPurity = 0.0
      t0 = System.currentTimeMillis()

      parameters._clusterMethod match {
        case "kmeans_h2o" => {
          val unsupervised = new Unsupervised(sc, sqlContext)
          if (parameters._cvClustering == true){
            val kTuning = unsupervised.kMeansTuning(trainingDS, responseColumn = "Region", ignoredColumns = Array("SampleId"), kSet = setK, nReapeat = parameters._nRepeatClustering)
            println("K estimation: ")
            kTuning.foreach{case(k, purity) => println(s"k = ${k}, purity = ${purity}")}
            k = kTuning.maxBy(_._2)._1
          }
          avgPurity = sc.parallelize(0 until parameters._nRepeatClustering).map{ _ =>
            val kMeansModel = unsupervised.kMeansH2O(ds, k)
            trainPrediction = unsupervised.kMeansPredict(kMeansModel, ds)
            clustering.purity(trainPrediction.select("Region", "Predict"))
          }.sum / parameters._nRepeatClustering
          // testPrediction = unsupervised.kMeansPredict(kMeansModel, testDS)
        }

        case "kmeans" => {
          if (parameters._cvClustering == true) {
            val kTuning = clustering.gmmKTuning(trainingDS, Array("SampleId", "Region"), setK, nReapeat = parameters._nRepeatClustering)
            println("K estimation: ")
            kTuning.foreach { case (k, purity) => println(s"k = ${k}, purity = ${purity}") }
            k = kTuning.maxBy(_._2)._1
          }
          avgPurity = sc.parallelize(0 until parameters._nRepeatClustering).map{ _ =>
            val kmeansModel = clustering.kmeansML(ds, "Region", "SampleId", k)
            trainPrediction = clustering.predict(kmeansModel, ds)
            clustering.purity(trainPrediction.select("Region", "Predict"))
          }.sum / parameters._nRepeatClustering
          // testPrediction = clustering.predict(gmmModel, testDS, Array("SampleId", "Region"))
        }

        case "gmm" => {
          if (parameters._cvClustering == true) {
            val kTuning = clustering.gmmKTuning(trainingDS, Array("SampleId", "Region"), setK, nReapeat = parameters._nRepeatClustering)
            println("K estimation: ")
            kTuning.foreach { case (k, purity) => println(s"k = ${k}, purity = ${purity}") }
            k = kTuning.maxBy(_._2)._1
          }
          avgPurity = sc.parallelize(0 until parameters._nRepeatClustering).map{ _ =>
            val gmmModel = clustering.gmm(ds, Array("SampleId", "Region"), k)
            trainPrediction = clustering.predict(gmmModel, ds, Array("SampleId", "Region"))
            clustering.purity(trainPrediction.select("Region", "Predict"))
          }.sum / parameters._nRepeatClustering
          // testPrediction = clustering.predict(gmmModel, testDS, Array("SampleId", "Region"))
        }

        case "bkm" => {
          if (parameters._cvClustering == true) {
            val kTuning = clustering.bkmKTuning(trainingDS, Array("SampleId", "Region"), setK, nReapeat = parameters._nRepeatClustering)
            println("K estimation: ")
            kTuning.foreach { case (k, purity) => println(s"k = ${k}, purity = ${purity}") }
            k = kTuning.maxBy(_._2)._1
          }
          avgPurity = sc.parallelize(0 until parameters._nRepeatClustering).map{ _ =>
            val bkmModel = clustering.bkm(ds, Array("SampleId", "Region"), k)
            trainPrediction = clustering.predict(bkmModel, ds, Array("SampleId", "Region"))
            clustering.purity(trainPrediction.select("Region", "Predict"))
          }.sum / parameters._nRepeatClustering
          // testPrediction = clustering.predict(bkmModel, testDS, Array("SampleId", "Region"))
        }
      }

      // var purity = clustering.purity(trainPrediction.select("Region", "Predict"))
      println($"Purity: ", avgPurity)
      /*      purity = clustering.purity(testPrediction.select("Region", "Predict"))
            println($"Test purity: ", purity)*/

      t1 = System.currentTimeMillis()
      val clusterTime = time.formatTimeDiff(t0, t1)
      println(s"Clustering. ${parameters._clusterMethod} : $clusterTime")
      timeResult ::= (s"Clustering. ${parameters._clusterMethod} : $clusterTime")

      timeResult.foreach(println)

      if (outputFilename != "null")
        trainPrediction.repartition(1).writeToCsv(outputFilename)
    }

    /**
      * Classification
      */

    split = ds.drop("SampleId").randomSplit(Array(0.7, 0.3), seeds)
    trainingDS = split(0)
    testDS  = split(1)

    if (parameters._isClassification == true) {
      println("Classification")
      val splitList = (0 until parameters._nRepeatClassification).map(_ => ds.drop("SampleId").randomSplit(Array(0.7, 0.3), seeds))
      val trainDfList = splitList.map(s => s(0))
      val testDfList = splitList.map(s => s(1))

      var trainingPrediction: DataFrame = null
      var testPrediction: DataFrame = null
      var trainingAvgError : Double = Double.NaN
      var testAvgError : Double = Double.NaN
      val classification = new Classification(sc, sqlContext)
      var pipelineModelsList : List[PipelineModel] = Nil

      t0 = System.currentTimeMillis()

      parameters._classificationMethod match {
        case "svm" => {
          println("Start svm")
          pipelineModelsList = trainDfList.map {trainingDf  =>
            val svmModel = classification.svm(trainingDf, cv = parameters._cvClassification, cost = Array(1), maxIter = Array(1))
            svmModel
          }.toList

          //          val dtStage = svmModel.stages(2).asInstanceOf[SVMModel]
          //          println(s"The best  learned classification SVM model: ${dtStage.mloutput}")
        }

        case "dt" => {
          println("Start dt")
          pipelineModelsList = trainDfList.map {trainingDf  =>
            val dtModel = classification.decisionTrees(trainingDS,  cv = parameters._cvClassification)//, "Region", 10, Array(1.0))
          val dtStage = dtModel.stages(2).asInstanceOf[DecisionTreeClassificationModel]
            println(s"The best  learned classification tree model: ${dtStage.toDebugString}")
            dtModel
          }.toList
        }

        case "rf" => {
          println("Start rf")
          pipelineModelsList = trainDfList.map { trainingDf =>
            val rfModel = classification.randomForest(trainingDS, cv = parameters._cvClassification)
            val dtStage = rfModel.stages(2).asInstanceOf[RandomForestClassificationModel]
            println(s"The best  learned classification random forest model: ${dtStage.toDebugString}")
            rfModel
          }.toList
        }
      }

      /**
        * Training prediction
        */
      var classificationMetrics = pipelineModelsList.zip(trainDfList).map { case (model, trainingDf) =>
        classification.predict(model, trainingDf)
        new ClassificationMetrics(classification._error, classification._precisionByLabel, classification._recallByLabel, classification._fScoreByLabel)
      }

      println(s"Classification algorithm was repeated ${parameters._nRepeatClassification} times")
      println("\nTraining evaluation: ")
      computeAvgMetrics(classificationMetrics)
      println()

      /**
        * Test prediction
        */
      classificationMetrics = pipelineModelsList.zip(testDfList).map { case (model, testDf) =>
        classification.predict(model, testDf)
        new ClassificationMetrics(classification._error, classification._precisionByLabel, classification._recallByLabel, classification._fScoreByLabel)
      }
      println("\nTest evaluation: ")
      computeAvgMetrics(classificationMetrics)

      t1 = System.currentTimeMillis()
      val classTime = time.formatTimeDiff(t0, t1)
      println(s"\nClassification. ${parameters._classificationMethod} : $classTime")
      timeResult ::= (s"Classification. ${parameters._classificationMethod} : $classTime")

      testPrediction = classification._prediction

      timeResult.foreach(println)
      if (outputFilename != "null")
        testPrediction.repartition(1).writeToCsv(outputFilename)
    }

    timeResult.foreach(println)
























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


