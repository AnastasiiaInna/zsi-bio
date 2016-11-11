/*
package com.zsibio

/**
  * Created by anastasiia on 11/11/16.
  */

import hex.pca.PCAModel.PCAParameters.Method
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{Genotype, GenotypeAllele}
import java.nio.file.{Paths, Files}
import scala.io.Source
import scala.collection.immutable.Range.inclusive

object ClusteringExec {

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
    unsupervisedModel.writecsv(kmeansPred, "kmeanspca_super_pop.csv")

  }

}

*/