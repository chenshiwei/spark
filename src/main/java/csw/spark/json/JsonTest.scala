package csw.spark.json

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 作用:
  *
  * @author chensw
  * @since 2018/10/25
  */
object JsonTest extends App {
  val conf = new SparkConf().setAppName("Spark Json").setMaster("local[4]")

  val spark = SparkSession
    .builder().config(conf)
    .getOrCreate()

  val df = spark.read.format("json").load("../spark/src/main/resources/people.json")

  df.show()
}
