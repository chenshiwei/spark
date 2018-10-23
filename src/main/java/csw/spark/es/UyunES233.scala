package csw.spark.es

import java.util.{Map => JMap}

import com.alibaba.fastjson.{JSON, TypeReference}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

object UyunES233 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark ES").setMaster("local[4]")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "10.1.53.233")
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
    import org.elasticsearch.spark._

    val maps = Seq(Map("id" -> 1, "name" -> "csw", "age" -> 23, "day" -> "2018-02-03"),
      Map("id" -> 2, "name" -> "csw2", "age" -> 24, "day" -> "2013-12-03"),
      Map("id" -> 3, "name" -> "csw3", "age" -> 25, "day" -> "2015-08-08"),
      Map("id" -> 4, "name" -> "csw4", "age" -> 26, "day" -> "2016-12-15"),
      Map("id" -> 5, "name" -> "csw5", "age" -> 27, "day" -> "2014-02-25"),
      Map("id" -> 6, "name" -> "csw6", "age" -> 28, "day" -> "2011-05-05"))
    spark.sparkContext.makeRDD(maps).saveToEs("spark/test")


    import scalaj.http.Http

    val json = Http("http://10.1.53.233:9200/_mapping").asString.body
println(json)
    val map = JSON.parseObject(json, new TypeReference[JMap[String, JMap[String, JMap[String, Any]]]]() {})
    map.asScala.foreach(kv=>kv._2.get("mappings").asScala.keys.foreach(typeName=>{
      spark.read.format("org.elasticsearch.spark.sql").load(s"${kv._1}/$typeName")
        .createOrReplaceTempView(s"${kv._1.replace("-","_")}_$typeName")
      println(s"${kv._1.replace("-","_")}_$typeName")

    }))




  }

}
