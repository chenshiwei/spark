package csw.spark.mysql

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import csw.spark.mysql.Test5.{rdd, spark}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 作用:
  *
  * @author chensw
  * @since 2020/3/12
  */
object Test10 extends App {
    val conf = new SparkConf().setAppName("Spark mysql").setMaster("local[4]")

    val spark = SparkSession
        .builder().config(conf)
        .getOrCreate()

    private val value: RDD[Event] = spark.read.option("header", value = true).csv("E:/tmp/hai_guan.csv").rdd.map(row => {
//        val time = row.getString(0).toLong + TimeUnit.DAYS.toMillis(234)
        Event(row.getString(0), stringToLong(row.getString(1))+TimeUnit.DAYS.toMillis(665)
            , row.getString(2).toDouble
        )
    })


    spark.createDataFrame(value)
        .write.format("jdbc")
        .mode(SaveMode.Append)
        .options(Map(
            "password" -> "DBuser123!",
            "driver" -> "com.mysql.jdbc.Driver",
            "dbtable" -> "hai_guan2",
            "user" -> "dbuser",
            "url" -> "jdbc:mysql://10.1.11.52:3306/ai"))
        .save()

    def stringToLong(strTime: String, format: String = "yyyy-MM-dd HH:mm:ss"): Long =
        stringToDate(strTime, format).getTime

    def stringToDate(dateString: String, format: String = "yyyy-MM-dd HH:mm:ss"): Date =
        new SimpleDateFormat(format).parse(dateString)

    case class Event(host_name: String, timestamp: Long, value: Double)
}
