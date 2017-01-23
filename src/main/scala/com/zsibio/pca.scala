package com.zsibio

/**
  * Created by anastasiia on 10/9/16.
  */

import java.nio.file.Files._

import hex.ModelMetrics
import hex.Model.Output
import hex.FrameSplitter
import hex.pca.PCA
import hex.kmeans.KMeans
import hex.kmeans.KMeansModel.KMeansParameters
import hex.pca.ModelMetricsPCA.PCAModelMetrics
import hex.pca.PCAModel.PCAParameters
import hex.pca.PCAModel.PCAParameters.Method
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.B
import org.apache.spark.h2o.H2OFrame
import org.apache.spark.{SparkConf, SparkContext}
import water.IcedWrapper
import org.apache.spark.h2o.H2OContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{Genotype, GenotypeAllele}
import water.Key
import water.fvec.Frame

import scala.collection.JavaConverters._
import scala.collection.immutable.Range.inclusive
import java.nio.file.{Paths, Files}
import scala.io.Source

object pcaReduction {
  def main(args: Array[String]): Unit = {
    //val genotypeFile = args(0)
    //val panelFile = args(1)

    case class SampleVariant(sampleId: String, variantId: Int, alternateCount: Int)

    val genotypeFile = "/home/anastasiia/1000genomes/ALL.chrMT.phase3_callmom-v0_4.20130502.genotypes.vcf.adam"
    val panelFile = "/home/anastasiia/1000genomes/ALL.panel"

    // val master = if (args.length > 2) Some(args(2)) else None
    val conf = new SparkConf().setAppName("PopStrat").setMaster("local")
      .set("spark.ext.h2o.repl.enabled", "false")
    // .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // .set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val h2oContext = H2OContext.getOrCreate(sc)
    import h2oContext._

    //    val populations = Array("GBR", "ASW", "CHB") //, "GWD")//, "YRI", "IBS", "TSI")
    val populations = Array("AFR", "EUR", "AMR")//, "EAS",  "SAS")

    def extract(file: String, superPop: String, filter: (String, String) => Boolean): Map[String,String] = {
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

    val panel: Map[String,String] = extract(panelFile, "super_pop",  (sampleID: String, pop: String) => populations.contains(pop))

    val allGenotypes: RDD[Genotype] = sc.loadGenotypes(genotypeFile)
    val genotypes: RDD[Genotype] = allGenotypes.filter(genotype => {
      panel.contains(genotype.getSampleId)
    })

    def variantId(genotype: Genotype): String = {
      val name = genotype.getVariant.getContig.getContigName
      val start = genotype.getVariant.getStart
      val end = genotype.getVariant.getEnd
      s"$name:$start:$end"
    }

    def alternateCount(genotype: Genotype): Int = {
      genotype.getAlleles.asScala.count(_ != GenotypeAllele.Ref)
    }

    def toVariant(genotype: Genotype): SampleVariant = {
      new SampleVariant(genotype.getSampleId.intern(), variantId(genotype).hashCode, alternateCount(genotype))
    }

    def getDataForDimReduction(variantsBySampleId: RDD[(String, Iterable[SampleVariant])], variantsByVariantId: RDD[(Int, Iterable[SampleVariant])], variantFrequencies: Map[Int, Float], infFreq: Double, supFreq: Double): (Frame, Frame) ={

      val filteredVariantsBySampleId: RDD[(String, Iterable[SampleVariant])] = variantsBySampleId.map {
        case (sampleId, sampleVariants) =>
          val filteredSampleVariants = sampleVariants.filter(variant => variantFrequencies.getOrElse(variant.variantId, -1).toString.toFloat >= infFreq && variantFrequencies.getOrElse(variant.variantId, -1).toString.toFloat <= supFreq)
          (sampleId, filteredSampleVariants)
      }

      val sortedVariantsBySampleId: RDD[(String, Array[SampleVariant])] = filteredVariantsBySampleId.map {
        case (sampleId, variants) =>
          (sampleId, variants.toArray.sortBy(_.variantId))
      }

      val header = StructType(
        Array(StructField("SampleId", StringType))++
          Array(StructField("Region", StringType)) ++
          sortedVariantsBySampleId.first()._2.map(variant => {StructField(variant.variantId.toString, IntegerType)}))

      val rowRDD: RDD[Row] = sortedVariantsBySampleId.map {
        case (sampleId, sortedVariants) =>
          val region: Array[String] = Array(panel.getOrElse(sampleId, "Unknown"))
          val alternateCounts: Array[Int] = sortedVariants.map(_.alternateCount)
          Row.fromSeq(Array(sampleId) ++ region ++ alternateCounts)
      }

      // Create the SchemaRDD from the header and rows and convert the SchemaRDD into a H2O dataframe

      val schemaRDD = sqlContext.createDataFrame(rowRDD, header)

      val dataFrame = h2oContext.asH2OFrame(schemaRDD)
      println("Git 6")
      dataFrame.replace(dataFrame.find("Region"), dataFrame.vec("Region").toCategoricalVec()).remove()
      dataFrame.update()

      println(dataFrame._key)
      dataFrame.vecs(Array.range(3, dataFrame.numCols())).map(i => i.toNumericVec)

      println("Git 7")

      val frameSplitter = new FrameSplitter(dataFrame, Array(.7), Array("training", "validation").map(Key.make[Frame](_)), null)
      water.H2O.submitTask(frameSplitter)
      val splits = frameSplitter.getResult
      val training = splits(0)
      val validation = splits(1)


      return (training, validation)
    }

    val variantsRDD: RDD[SampleVariant] = genotypes.map(toVariant)

    val variantsBySampleId: RDD[(String, Iterable[SampleVariant])] = variantsRDD.groupBy(_.sampleId).cache()
    //val sampleCount: Long = variantsBySampleId.count()
    val sampleCount: Long = 1510
    println("Found " + sampleCount + " samples")

    val variantsByVariantId: RDD[(Int, Iterable[SampleVariant])] = variantsRDD.groupBy(_.variantId).filter {
      case (_, sampleVariants) => sampleVariants.size == sampleCount
    }

    val variantFrequencies: Map[Int, Float] = variantsByVariantId.map {
      case (variantId, sampleVariants) => (variantId, sampleVariants.count(_.alternateCount > 0) / sampleCount.toFloat)
    }.collect().toMap

    val infFreq = 0.02
    val supFreq = 0.05

    println("Git 4")

    val trainValidDF = getDataForDimReduction(variantsBySampleId, variantsByVariantId, variantFrequencies, infFreq, supFreq)

    println("Git 5")

    val training = trainValidDF._1
    val validation = trainValidDF._2

    print("------------ Cardinality: -------------------\n")

    val nFeatures = training.numCols() - 2 // remove SampleId and Region
    val nObservations = training.numRows().toInt
    print(nFeatures, nObservations)

    val pcaParameters = new PCAParameters()
    pcaParameters._train = training._key
    pcaParameters._valid = validation._key
    pcaParameters._response_column = "Region"
    pcaParameters._ignored_columns = Array("SampleId")
    pcaParameters._k = math.min(nFeatures, nObservations)
    pcaParameters._use_all_factor_levels = true
    // pcaParameters._pca_method = Method.GLRM
    pcaParameters._max_iterations = 100

    val pcaObject = new PCA(pcaParameters)
    val pcaModel = pcaObject.trainModel.get

    val pcaImportance = pcaModel._output._importance
    val pcaCumVariance = pcaImportance.getCellValues.toList(2).toList
    val pcaEigenvectors = pcaModel._output._eigenvectors

    print("------------ Total variation: ----------------\n")
    print(pcaImportance.toString)


    val totalVariancePerc : Double = .5

    val intPcaCumVariance = pcaCumVariance.map(p => p.get().asInstanceOf[Double])
    val numberPC = 3//intPcaCumVariance.filter(x => x <= totalVariancePerc).size

    val prediction = pcaModel.score(training)
    val pcaDs = prediction

    pcaDs.remove(Array.range(numberPC, pcaEigenvectors.getColDim))
    pcaDs.update()

    pcaDs.add(Array("SampleId", "Region"), Array(training.vec("SampleId").toCategoricalVec(), training.vec("Region").toCategoricalVec()))
    pcaDs.update()

    print("\n------------ Training data after PCA ----------- \n")
    print(pcaDs._key.get())

    val numberOfClusters = populations.size
    val frameSplitterPca = new FrameSplitter(pcaDs, Array(.7), Array("training", "validation").map(Key.make[Frame](_)), null)
    water.H2O.submitTask(frameSplitterPca)
    val splitsPca = frameSplitterPca.getResult
    val trainingPca = splitsPca(0)
    val validationPca = splitsPca(1)

    val kmeansParameters = new KMeansParameters()
    kmeansParameters._train = trainingPca._key
    kmeansParameters._response_column = "Region"
    kmeansParameters._ignored_columns = Array("SampleId")
    kmeansParameters._k = numberOfClusters

    val kmeans = new KMeans(kmeansParameters)
    val kmeansModel = kmeans.trainModel().get()

    // Score the model against the entire dataset (training, test, and validation data)
    // This causes the confusion matrix to be printed

    val kmeansPrediction = kmeansModel.score(trainingPca)//('predict)

    print(kmeansPrediction._key.get())

    val outputPcaKmeans = trainingPca
    outputPcaKmeans.add("Predict", kmeansPrediction.vec("predict").toCategoricalVec)
    outputPcaKmeans.update()

    print(outputPcaKmeans._key.get())
    // print(outputPcaKmeans.subframe(Array("SampleId", "Region", "Predict")).vec("Predict").equals(0))

   // val h2oContext = H2OContext.getOrCreate(sc)
   // import h2oContext._

    val kmeansPred = asDataFrame(h2oContext.asH2OFrame(outputPcaKmeans))(sqlContext)
    //kmeansPred.select("SampleId", "Region", "Predict").filter(kmeansPred("Predict") === 0).collect().foreach(println)



    // val rdd = sc.parallelize(Seq(kmeansPred.select("SampleId", "Region", "Predict").filter(kmeansPred("Predict") === 0), kmeansPred.select("SampleId", "Region", "Predict").filter(kmeansPred("Predict") === 1)))


    // kmeansPred.registerTempTable("kmeansPred")
    val clusters = inclusive(0, numberOfClusters - 1)
    var cluster = 0
    for (cluster <- clusters){
      println("Iteration: " + cluster)
      val sampleIdInCluster = kmeansPred.filter(kmeansPred("Predict") === cluster).select("SampleId", "Region", "Predict")//sqlContext.sql("SELECT SampleID, Region, Predict FROM kmeansPred WHERE Predict = cluster")
      val sampleIdSet = kmeansPred.filter(kmeansPred("Predict") === cluster).select("SampleId").collect().map(row => row.mkString)

      /*print(sampleIdSet)
      sampleIdInCluster.collect().foreach(println)*/

      val panelSubPopulation: Map[String,String] = extract(panelFile, "pop", (sampleID: String, pop: String) => sampleIdSet.contains(sampleID))
      print(panelSubPopulation.toString())

      val genotypesSubPop : RDD[Genotype] = genotypes.filter(genotype => panelSubPopulation.contains(genotype.getSampleId))
      val variantsBySampleIdSubPop: RDD[(String, Iterable[SampleVariant])] = variantsBySampleId.filter(genotype => panelSubPopulation.contains(genotype._1))
      val sampleCount: Long = variantsBySampleId.count()

      val variantsByVariantId: RDD[(Int, Iterable[SampleVariant])] = variantsRDD.groupBy(_.variantId).filter {
        case (_, sampleVariants) => sampleVariants.size == sampleCount
      }

      val variantFrequencies: Map[Int, Float] = variantsByVariantId.map {
        case (variantId, sampleVariants) => (variantId, sampleVariants.count(_.alternateCount > 0) / sampleCount.toFloat)
      }.collect().toMap



      val trainValidDFsub = getDataForDimReduction(variantsBySampleId, variantsByVariantId, variantFrequencies, infFreq, supFreq)

      println("Git ok")

      // val variantsSubPopRDD: RDD[SampleVariant] = variantsRDD.filter(variant => panelSubPopulation.contains(variant.sampleId))
      // println(variantsSubPopRDD.first())
    }


    /*

    val inputParquetFile = "/home/anastasiia/IdeaProjects/prediction.parquet"
    Files.deleteIfExists(Paths.get(inputParquetFile))
    kmeansPred.write.parquet(inputParquetFile)

    val parquetFile = sqlContext.read.parquet("/home/anastasiia/IdeaProjects/prediction.parquet")

    parquetFile.registerTempTable("kmeansPred")
*/

/*
    val outputPredictionFile = "/home/anastasiia/IdeaProjects/outputPcaKmeans.txt"
    Files.deleteIfExists(Paths.get(outputPredictionFile))

    kmeansPred.registerTempTable("kmeansPred")
    val df = sqlContext.sql("SELECT * FROM kmeansPred")
    df.write.format("com.databricks.spark.csv").save(outputPredictionFile)


    val kmeansTestPrediction = kmeansModel.score(validationPca)//('predict)

    print(kmeansTestPrediction._key.get())

    val outputPcaTestKmeans = validationPca
    outputPcaTestKmeans.add("Predict", kmeansTestPrediction.vec("predict").toCategoricalVec)
    outputPcaTestKmeans.update()

    print(outputPcaTestKmeans._key.get())

    val kmeansTestPred = asDataFrame(h2oContext.asH2OFrame(outputPcaTestKmeans))(sqlContext)


    val outputTestPredictionFile = "/home/anastasiia/IdeaProjects/outputTestPcaKmeans.txt"
    kmeansTestPred.registerTempTable("kmeansTestPred")
    val dfTest = sqlContext.sql("SELECT * FROM kmeansTestPred")
    dfTest.write.format("com.databricks.spark.csv").save(outputTestPredictionFile)

*/

    print("\n---------- Ok ----------\n")

  }
}
