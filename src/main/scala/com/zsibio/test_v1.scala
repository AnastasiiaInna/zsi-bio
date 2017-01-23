package com.zsibio
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{Genotype}
import org.apache.spark.h2o._
import water.Key
import scala.collection.JavaConverters._
import scala.collection.immutable.Range.inclusive
import scala.io.Source

import scala.collection.mutable.ArrayBuffer
import java.io.File


object Test {
  def getListOfFiles(directoryName: String): List[String] = {
    val directory = new File(directoryName)
    val files = directory.listFiles // this is File[]

    val fileNames = ArrayBuffer[String]()
    for (file <- files)
      fileNames += file.getName()

    return fileNames.toList
  }

  def main(args: Array[String]): Unit = {
    val dataDir = "/home/anastasiia/IdeaProjects/1000genomes/"
    val adamDataList = getListOfFiles(dataDir)

    val conf = new SparkConf().setAppName("test_v1").setMaster("local").set("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)

    val chrMT = adamDataList(1)

    val genotypeFile = "/home/anastasiia/ADAMFiles/ALL.chrMT.phase3_callmom-v0_4.20130502.genotypes.vcf.adam"
    val panelFile = "/home/anastasiia/ADAMFiles/ALL.panel"

        def extract(file: String, filter: (String, String) => Boolean): Map[String,String] = {
      Source.fromFile(file).getLines().map(line => {
        val tokens = line.split("\t").toList
        tokens(0) -> tokens(1)
      }).toMap.filter(tuple => filter(tuple._1, tuple._2))
    }

    val populations = Set("GBR", "ASW", "CHB")
    val panel: Map[String,String] = extract(panelFile, (sampleID: String, pop: String) => populations.contains(pop))

    print(panel)

    val allGenotypes: RDD[Genotype] = sc.loadGenotypes(genotypeFile)
    val genotypes: RDD[Genotype] = allGenotypes.filter(genotype => {panel.contains(genotype.getSampleId)})

    //print(" ------ Size is " + genotypes.count() + " ------ ")
    print(chrMT)
  }
}
