package csw.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * @author chensw
  * @since at 2016-11-30下午3:04:46
  */
object SQLCSV {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder().master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()

    //    1535731200000L.to(1536940500000L,300000L).foreach(l=>
    //      println(l,spark.read.csv(s"D:\\tmp\\2019AIOps_data_(20190131update)\\$l.csv").rdd.map(_.getString(5).toLong).sum()))
    // For implicit conversions like converting RDDs to DataFrames
    //    1536940800000L.to(1538150100000L,300000L).foreach(l=>
    //    println(l,spark.read.csv(s"D:\\tmp\\2019AIOps_data_test1\\$l.csv").rdd.map(_.getString(5).toDouble).sum()))
    1538150400000L.to(1539359700000L, 300000L).foreach(l =>
      println(l, spark.read.csv(s"D:\\tmp\\2019AIOps_data_test2\\$l.csv").rdd.map(_.getString(5).toDouble).sum()))
  }
}
