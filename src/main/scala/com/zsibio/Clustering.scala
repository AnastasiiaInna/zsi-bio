package com.zsibio

/**
  * Created by anastasiia on 10/6/16.

  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{Genotype, GenotypeAllele}
import java.nio.file.{Paths, Files}
import scala.io.Source
import scala.collection.immutable.Range.inclusive

object Clustering {

  def main(args: Array[String]): Unit = {

    var genotypeFile: String = "/home/anastasiia/1000genomes/ALL.chrMT.phase3_callmom-v0_4.20130502.genotypes.vcf.adam" //"/home/anastasiia/1000genomes/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.adam"
    var panelFile: String = "/home/anastasiia/1000genomes/ALL.panel"

    if (args.length == 2){
      genotypeFile = args(0)
      panelFile = args(1)
    }

    val conf = new SparkConf().setAppName("PopStrat").setMaster("local").set("spark.ext.h2o.repl.enabled", "false")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    /*
        val gtsForSample: RDD[Genotype] = sc.loadGenotypes(genotypeFile)
        val start = 16000000
        val end   = 16500000
        val sampledGts = gtsForSample.filter(g => (g.getVariant.getStart >= start && g.getVariant.getEnd <= end) )
        sampledGts.adamParquetSave("/home/anastasiia/1000genomes/chr22-sample.adam")
       */

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

    val panel: Map[String, String] = extract(panelFile, "super_pop", (sampleID: String, pop: String) => populations.contains(pop))

    val allGenotypes: RDD[Genotype] = sc.loadGenotypes(genotypeFile)
    val genotypes: RDD[Genotype] = allGenotypes.filter(genotype => {
      panel.contains(genotype.getSampleId)
    })

    val gts = new Population(sc, sqlContext, genotypes, panel)
    val ldPrun = new LDPruning(sc, sqlContext)
    val unsupervisedModel = new Unsupervised(sc, sqlContext)

    // val pcaMethods = Set(Method.GLRM, Method.GramSVD)//, Method.Power, Method.Randomized)

    val variantsRDD = gts.sortedVariantsBySampelId
    val snpIdSet: List[String] = ldPrun.performLDPruning(variantsRDD)
    val ds = gts.getDataSet(variantsRDD, snpIdSet)
    println(ds.columns.length, ds.count())

    val pcaDS = unsupervisedModel.pcaH2O(ds)
    val kmeansPred = unsupervisedModel.kMeansH2OTrain(pcaDS, populations.size)
    kmeansPred.foreach(println)
    unsupervisedModel.writecsv(kmeansPred, "/home/anastasiia/IdeaProjects/kmeanspca_super_pop.csv")

/*
    val snpIdSet = gts.performLDPruning()
    val ds = gts.getDataSet(gts.sortedVariantsBySampelId, snpIdSet)
    println(ds.columns.length, ds.count())
    println(ds.columns.toString)

    val pcaDS = unsupervisedModel.pcaH2O(ds)
    println("pcaDS cardinality: ", pcaDS.keys().length, pcaDS.keys().size)
    val kmeansPred = unsupervisedModel.kMeansH2OTrain(pcaDS, populations.size)
    kmeansPred.foreach(println)
    unsupervisedModel.writecsv(kmeansPred, "/home/anastasiia/IdeaProjects/kmeanspca_super_pop.csv")*/

    /*    val snpsetId = gts.snpIdSet
        val snpSet = gts.snpSet

        println(snpsetId.size)
        println(snpSet.first().size)
        println(snpSet.collect()(0).size)
        println(snpSet.collect()(0))
        println(ds.columns.size, ds.count(), ds.select(snpsetId(0)).count())*/

    /*    val snp1 = Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0)
        val snp2 = Vector(0, 2, 0, 2, 2, 1, 1, 1, 0, 0, 2, 0, 2, 2, 1, 1, 1, 3)
        println("cor :")
        val corr = gts.pairComposite(snp1, snp2)
        println(corr)
    */

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
/*    val clusters = inclusive(0, populations.length - 1)
    val sampleIdInCluster = clusters.map(cluster => kmeansPred.filter(kmeansPred("Predict") === cluster).select("SampleId", "Region", "Predict"))
    val sampleIdSet = clusters.map(cluster => kmeansPred.filter(kmeansPred("Predict") === cluster).select("SampleId").collect().map(row => row.mkString))*/


/*      sampleIdSet.foreach(ds => {
      println("New one")
      ds.foreach(println)
    }
    )*/





/*    val panelsSubPopulation = sampleIdSet.map(samples => extract(panelFile, "pop", (sampleID: String, pop: String) => samples.contains(sampleID)))
    val genotypesSubPop = panelsSubPopulation.map(panelSubPopulation => genotypes.filter(genotype => panelSubPopulation.contains(genotype.getSampleId)))

    val subGts = (genotypesSubPop, panelsSubPopulation).zipped.map{(subPop, panelSubPop) => {
      var supFreq = 1.0
      var newPop = new Population(sc, sqlContext, subPop, panelSubPop, 0.001, supFreq)
      val snpIdSet = newPop.performLDPruning()
      val dataSet = newPop.getDataSet(newPop.sortedVariantsBySampelId, snpIdSet)
      while (dataSet.columns.length == 2){
        supFreq += 0.001
        newPop = new Population(sc, sqlContext, subPop, panelSubPop, 0.001, supFreq)
      }
      newPop}
    }

    val kmeansPredictions = subGts.map{genotype =>
      println("\nNew SubPopulation: \n")
      // println(genotype.dataSet.foreach(println))
      val pcaDS = unsupervisedModel.pcaH2O(genotype._dataSet)
      unsupervisedModel.kMeansH2OTrain(pcaDS, genotype._numberOfRegions)
    }
    (clusters, kmeansPredictions).zipped.map{(cluster, prediction) => unsupervisedModel.writecsv(prediction, s"/home/anastasiia/IdeaProjects/chr22/kmeanspca_$cluster.csv")}*/









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

