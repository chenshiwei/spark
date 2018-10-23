package csw.spark.s2

import org.apache.spark.sql.types.{ DoubleType, StringType, StructField, StructType }
import org.apache.spark.sql.{ Row, SparkSession }
import scala.tools.cmd.Spec.Info
import org.apache.parquet.it.unimi.dsi.fastutil.Hash
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders

/**
 * RDD API to Dataset API
 * http://www.iteblog.com
 */
object RDDToDataSet3 extends App {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("example")
    .getOrCreate()

//  val test = sparkSession.read.option("header", "true").csv("E:/tmp/heatmap_coordinates/part-00000")
//
//  import sparkSession.implicits._
//  val testds = test.map(AppInfo2(_))
//  println(testds.count())
//  test.take(1).foreach { println }
//  test.show()
//  testds.show(50)

}