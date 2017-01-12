import scala.util.Properties

name := """zsi-bio"""

version := "1.0"

scalaVersion := "2.10.4"

val DEFAULT_SPARK_VERSION = "1.6.2"
val DEFAULT_HADOOP_VERSION = "2.7.1"

lazy val sparkVersion = Properties.envOrElse("SPARK_VERSION", DEFAULT_SPARK_VERSION)
lazy val hadoopVersion = Properties.envOrElse("SPARK_HADOOP_VERSION", DEFAULT_HADOOP_VERSION)

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "3.0.0-M15" % "test",
  "org.apache.spark" % "spark-core_2.10" % sparkVersion % "provided",
  "org.apache.spark" % "spark-sql_2.10" % sparkVersion,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
  "org.bdgenomics.adam" % "adam-core" % "0.16.0"
    exclude("org.apache.spark", "spark-core_2.10")
    exclude("org.apache.hadoop", "hadoop-client")
    exclude("org.bdgenomics.utils", "utils-metrics_2.10"),
  "org.bdgenomics.utils" % "utils-misc_2.10" % "0.2.3"
    exclude("org.apache.spark", "spark-core_2.10")
    exclude("org.apache.hadoop", "hadoop-client"),
  "org.seqdoop" % "hadoop-bam" % "7.2.1"
    exclude("org.apache.hadoop", "hadoop-client"),
  "spark.jobserver" % "job-server-api_2.10" % "0.5.2",
  "ai.h2o" % "sparkling-water-core_2.10" % sparkVersion,
  "ai.h2o" % "h2o-algos" % "3.8.2.11",
  "org.apache.systemml" % "systemml" % "0.11.0-incubating"
    exclude("org.apache.spark", "spark-core_2.10")
    exclude("org.apache.hadoop", "hadoop-client")
    exclude("org.apache.hadoop", "hadoop-mapreduce-client-core")
    exclude("org.apache.hadoop", "hadoop-mapreduce-client-app")
    exclude("org.apache.hadoop", "hadoop-mapreduce-client-jobclient")
    exclude("org.apache.hadoop", "hadoop-yarn-api"),
  "com.databricks" % "spark-csv_2.10" % "1.5.0" % "provided",
  "com.databricks" % "spark-csv_2.10" % "1.5.0",
  "com.github.haifengl" % "smile-core" % "1.2.0",
  "com.github.karlhigley" %% "spark-neighbors" % "0.2.2"
  // "org.ddahl" % "rscala_2.11" % "1.0.13"

)

/*resolvers ++= Seq(
  "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven",
  "OSS Sonatype" at "https://repo1.maven.org/maven2/"
)*/

parallelExecution in Test := false
fork := true

mainClass in assembly := Some("com.zsibio.PopulationStratification")
assemblyJarName in assembly := "zsi-bio-popStrat.jar"

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "hadoop", "yarn", xs@_*) => MergeStrategy.first
  case PathList("org", "apache", "commons", xs@_*) => MergeStrategy.first
  case ("git.properties") => MergeStrategy.concat
  case ("log4j.properties") => MergeStrategy.concat
  case x => (assemblyMergeStrategy in assembly).value(x)
}


/*lazy val copyDocAssetsTask = taskKey[Unit]("Copy doc assets")

copyDocAssetsTask := {
  val sourceDir = file("resources/doc-resources")
  val targetDir = (target in(Compile, doc)).value
  IO.copyDirectory(sourceDir, targetDir)
}

copyDocAssetsTask <<= copyDocAssetsTask triggeredBy (doc in Compile)*/

// net.virtualvoid.sbt.graph.Plugin.graphSettings

