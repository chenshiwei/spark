package csw.spark.sql

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author chensw
  * @since at 2016-11-30下午3:04:46
  */
object SQLCassandraTest {

  var sc: SparkContext = _
  var spark: SparkSession = _
  var keySpace: String = "guang_da"

  def main(args: Array[String]): Unit = {
    connect()
    "SELECT sum(memory_predict),last(node) FROM jixian WHERE type_name = 'vm_stats'".show()
    Thread.sleep(1000000)
  }

  def connect(host: String = "10.1.50.240") {
    val sparkConf = new SparkConf().setMaster("local[8]").setAppName("sql-test")
      .set("spark.cassandra.connection.host", host)
      .set("spark.cassandra.auth.username", "root")
      .set("spark.cassandra.auth.password", "Root_123")

    if (sparkConf.get("spark.master").startsWith("spark://")) {
      sparkConf.setJars(new File("lib/").listFiles()
        .map(_.getAbsolutePath)
        .filter(_.endsWith(".jar")))
    }
    spark = SparkSession.builder().config(sparkConf).getOrCreate()
    spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "system_schema", "table" -> "tables"))
      .load.createOrReplaceTempView("all_tables")
    spark.sql(s"select table_name from all_tables where keyspace_name='$keySpace'")
      .collect().foreach(row => {
      spark.read.format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace" -> keySpace, "table" -> row.getString(0)))
        .load.createOrReplaceTempView(row.getString(0))
    })
    spark.sql("select 1")
    println("connect success")

  }

  implicit def convertToDF(sql: String): DataFrame = {
    try {
      spark.sql(sql)
    } catch {
      case e: Exception =>
        null
    }
  }
}



