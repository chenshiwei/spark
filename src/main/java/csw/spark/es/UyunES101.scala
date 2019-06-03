package csw.spark.es

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object UyunES101 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark ES").setMaster("local[4]")
//    conf.set("es.index.auto.create", "true")
//    conf.set("es.nodes", "10.1.61.101")
//    conf.set("es.port", "9200")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    implicit def convertToDF(sql: String): DataFrame = {
      try {
        spark.sql(sql)
      } catch {
        case e: Throwable =>
          println(e.getMessage)
          null
      }
    }

      spark.read.format("org.elasticsearch.spark.sql")
      .options(Map(    "es.index.auto.create"-> "true",
        "es.nodes"-> "10.1.50.56",
        "es.port"->"9200"))
        .load("index_*/type_3")
        .createOrReplaceTempView("tmp")
//      s"SELECT * FROM nbank_ebip".repartition(1).write.option("header", "true").option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
//      .csv("F:/tmp/nbank")
//      s"SELECT count(1) FROM nbank_ebip".write.format("org.elasticsearch.spark.sql")
//         .options(Map(    "es.index.auto.create"-> "true",
//      "es.nodes"-> "10.1.61.101",
//      "es.port"->"9200")).save("spark_es/test")

//"""
//  |select metric_name,avg(cast(metric_value as double)) as sl from e10adc3949ba59abbe56e057f20f88dd_metric_datapoint_20181130_7 group by metric_name
//""".stripMargin.show()
//    Thread.sleep(100000)
"SELECT * FROM tmp".show()

    //    spark.udf.register("strptime", (str: String) => stringToLong(str, "yyyy-MM-dd HH:mm:ss"))

    //    "SELECT *,strptime(occur_time) FROM spider_event ORDER BY occur_time".show(false)
    //      .repartition(1).write.option("header", "true").csv("E:/tmp/spider_event2")

    //    "SELECT `@COLLECT_IP`,userName,logTimestamp,type,TIME FROM e10adc3949ba59abbe56e057f20f88dd_linux_20180821_bs_system".show(1000,false)


  }

}
