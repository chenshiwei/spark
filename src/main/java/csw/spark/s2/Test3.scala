package csw.spark.s2

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
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

  val spark = SparkSession.builder.
    master("local")
    .appName("example")
    .getOrCreate()

  import org.apache.spark.sql.types._

  val data = Array(("464", "2", "3", "4", "5"), ("hu", "7", "8", "9", "10"))
  val df = spark.createDataFrame(data).toDF("col1", "col2", "col3", "col4", "col5")

  import org.apache.spark.sql.functions._

  df.printSchema()
  val colNames = df.columns
  var df1 = df
  //  val cols = colNames.map(f => col(f).cast(DoubleType))
  for (colName <- colNames) {

    val tmp = df.withColumn(colName, col(colName).cast(DoubleType))
    if (tmp.select(colName).collect().map(_.getAs[Double](colName) != null).reduce(_ && _)) df1 = tmp

  }
  df1.show()


}