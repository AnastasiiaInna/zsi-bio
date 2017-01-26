import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import scala.collection.mutable.WrappedArray


val df1 = sqlContext.read.parquet("...")
df1.registerTempTable("gts")
val df2 = sqlContext.sql("select * from gts where sampleId in ('NA18871', 'NA18871', 'NA20760', 'HG01855', 'HG03024', 'HG02133', 'HG01198', 'HG00351', 'HG01242', 'HG02089', 'NA18989')").cache

case class Genotypes(sampleId: String, genotype: Int)
val makeGenotypes = udf((sampleId: String, genotype: WrappedArray[String]) => Genotypes(sampleId, {if(genotype(0) == "Alt" && genotype(1) == "Alt") 2 else if (genotype(0) == "Alt" || genotype(1)== "Alt")  1 else 0} ) )
val df3 = df2.withColumn("genotypes", makeGenotypes(col("sampleId"), col("alleles")))
df3.registerTempTable("gts")

val sampleCnt =  sqlContext.sql("select count (distinct sampleId) from gts").first.getLong(0).toInt
val df4 = sqlContext.sql(s"""
select variant.contig.contigName, variant.start,
count(*) as nonmissing,
variant.referenceAllele as ref,
variant.alternateAllele as alt,
sum(case when alleles[0] = 'Alt' and alleles[1] = 'Alt' then 2 when alleles[0] = 'Alt' or alleles[1] = 'Alt' then 1  else 0 end) as frequency,
collect_set( concat(genotypes.sampleId,":",cast(genotypes.genotype as string) ))
from gts
where 
length(variant.alternateAllele) = 1 and length(variant.referenceAllele) = 1
group by variant.contig.contigName, variant.start, variant.referenceAllele,variant.alternateAllele
having count(distinct variant.alternateAllele)  = 1  and count(*) = ${sampleCnt}
""")
val schema = df4.schema
val rows =df4.rdd.zipWithUniqueId().map{case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)}
val df5 = sqlContext.createDataFrame(rows, StructType(StructField("snpIdx", LongType, false) +: schema.fields))
///dump to parquet

case class TSNP(snpIdx: Long, snpId: String, snpPos: Int, snp: Vector[Int]){}
val df6 = df5.map {r => TSNP(r.getLong(0),s"${r.getString(1)}:${r.getLong(2).toString}",r.getLong(2).toInt,r.getAs[WrappedArray[String]](7).map(_.split(':')).map(r=>(r(0),r(1) )).sortBy(r=>r._1).map(_._2.toInt ).toArray )  }

// ldPrun.performLDPruning(df6, parameters._ldMethod, parameters._ldTreshold, parameters._ldMaxBp, parameters._ldMaxN)


/*and count(*) = sampleNum*/ 


/*
//PCA filtering
def myFun= udf(
    (originalColumnContent : Array[String]) =>  {
    filter .. [1:n]
      // do something with your original column content and return a new one
    }
  )
 
val newDf = myDf.withColumn("newCol1", myFun(myDf("originalColumn"))).drop(myDf("originalColumn"))

*/
  
  
  
  
  
  
  
  
  
  
