package csw.spark.es

import java.text.SimpleDateFormat
import java.util.Date
import java.util.{Map => JMap}

import com.alibaba.fastjson.{JSON, TypeReference}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object UyunES {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark ES").setMaster("local[4]")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "10.1.61.101")
    conf.set("es.port", "9200")

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

    def stringToLong(strTime: String, format: String): Long =
      stringToDate(strTime, format).getTime

    def stringToDate(dateString: String, format: String): Date =
      new SimpleDateFormat(format).parse(dateString)


    import scalaj.http.Http

    val json = Http("http://10.1.61.101:9200/_mapping/").asString.body
    println("sjij7987")
    println(json)

    val map = JSON.parseObject(json, new TypeReference[JMap[String, JMap[String, JMap[String, Any]]]]() {})
    println(map)
//    map.asScala.foreach(kv=>kv._2.get("mappings").asScala.keys.foreach(typeName=>{
//      println(s"${kv._1}/$typeName")
//      spark.read.format("org.elasticsearch.spark.sql").load(s"${kv._1}/$typeName")
//        .createOrReplaceTempView(s"${kv._1.replace("-","_")}_$typeName")
//
//    }))
//
//    spark.udf.register("strptime", (str: String) => stringToLong(str, "yyyy-MM-dd HH:mm:ss"))

//    "SELECT *,strptime(occur_time) FROM spider_event ORDER BY occur_time".show(false)
    //      .repartition(1).write.option("header", "true").csv("E:/tmp/spider_event2")

//    "SELECT `@COLLECT_IP`,userName,logTimestamp,type,TIME FROM e10adc3949ba59abbe56e057f20f88dd_linux_20180821_bs_system".show(1000,false)


  }

}
