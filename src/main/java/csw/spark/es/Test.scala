package csw.spark.es

import java.text.SimpleDateFormat
import java.util.{Date, Map => JMap}

import com.alibaba.fastjson.{JSON, TypeReference}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object Test {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark ES").setMaster("local[4]")
//    conf.set("es.index.auto.create", "true")
//    conf.set("es.nodes", "localhost")
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

    def stringToLong(strTime: String, format: String): Long =
      stringToDate(strTime, format).getTime

    def stringToDate(dateString: String, format: String): Date =
      new SimpleDateFormat(format).parse(dateString)


    import scalaj.http.Http

    val json = Http("http://localhost:9200/_mapping").asString.body

    val map = JSON.parseObject(json, new TypeReference[JMap[String, JMap[String, JMap[String, Any]]]]() {})
    println(json)
    val tableMap: ArrayBuffer[String] = ArrayBuffer()
    for ((k, v) <- map.asScala) {
      for ((k2, _) <- v.get("mappings").asScala) {
        tableMap.append(s"${k.replace("-","_")}_${k2.replace("-","_")}")
        println(s"${k.replace("-","_")}_${k2.replace("-","_")}")
        val df = spark.read.option("es.nodes", "localhost:9200").format("org.elasticsearch.spark.sql").load(s"$k/_doc")
        //        println(df.count())
        df.createOrReplaceTempView(s"${k.replace("-","_")}_${k2.replace("-","_")}")
      }
    }

//        val game = Map("media_type"->"game","title" -> "FF VI","year" -> "1994")
//        val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
//        val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")
//
//        spark.sparkContext.makeRDD(Seq(game, book, cd)).saveToEs("my-collection-{media_type}/doc")

    //    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    //    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
    //
    //    spark.sparkContext.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")

    //    spark.read.format("org.elasticsearch.spark.sql").load("entry_workflow/base")
    //      .createOrReplaceTempView("entry_workflow")
    //    spark.udf.register("time_to_long", (timeStr: String) => stringToLong(timeStr, "yyyy/MM/dd HH:mm:spark"))
    //
    //    val df: DataFrame =
    //      """
    //        |SELECT *
    //        |FROM entry_workflow
    //        |WHERE time_to_long("2017/2/7 00:12:00")<=time_to_long(CREATE_DATE)
    //        |AND time_to_long(CREATE_DATE)<time_to_long("2017/2/7 0:13:00")
    //      """.stripMargin
    //
    //    df.explain()
    //    df.write.format("org.elasticsearch.spark.sql")
    //      .option("es.resource", "entry_workflow_20170207001200/base")
    //      .mode(SaveMode.Overwrite).save()
    //    "Select * from entry_workflow".show()
    //    val df=spark.read.format("org.elasticsearch.spark.sql").load("_all")
    //    df.show()

    //    "SELECT * FROM comptrollerlogindex_comptrollerlogtype".show()
//
//    for (table <- tableMap) {
//      try {
//        println(s"$table")
//        s"DESC $table".show(false)
//        s"SELECT count(1) FROM $table".show()
//        //        s"SELECT * FROM $table".show()
//      } catch {
//        case _: Exception => println(s"$table is ERROR")
//      }
//    }

  }

}
