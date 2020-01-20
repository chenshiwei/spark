package csw.spark.s2

import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

/**
  * RDD API to Dataset API
  * http://www.iteblog.com
  */
object RDDToDataSet3 extends App {

    val spark = SparkSession.builder.
        master("local[8]")
        .appName("chensw")
        .getOrCreate()

    val rdd = spark.sparkContext.makeRDD(Array(1, 2, 3))
    spark.sparkContext.addFile("hdfs://10.1.62.12:19000/uyun/databank/spark/cert/keyStore.txt_1575553586460")
    rdd.foreach(println)

    println(SparkFiles.get("keyStore.txt_1575553586460"))

    Thread.sleep(100000)
}