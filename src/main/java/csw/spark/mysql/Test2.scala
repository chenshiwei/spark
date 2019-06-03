package csw.spark.mysql

import csw.spark.mysql.Demo.capacityAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 作用:
  *
  * @author chensw
  * @since 2018/10/26
  */
object Test2 extends App {
  val conf = new SparkConf().setAppName("Spark mysql").setMaster("local[4]")

  val spark = SparkSession
    .builder().config(conf)
    .getOrCreate()


   spark.read
   .format("jdbc").options(Map(
      "password"->"123456",
      "driver"->"com.mysql.jdbc.Driver",
      "dbtable"->"mock",
      "user"->"root",
      "url"->"jdbc:mysql://10.1.53.233:3306/dbgirl")).load().createOrReplaceTempView("tmp")

  var array = spark
    .sql(
      """
        | SELECT
        | SUM(mem_used)*100.0/SUM(mem_total) AS mem,
        | SUM(cpu_used)*100.0/SUM(cpu_total) AS cpu,
        | SUM(disk_used)*100.0/SUM(disk_total) AS disk
        | FROM tmp
        | GROUP BY date,service_id
        | ORDER BY date
      """.stripMargin)
    .rdd.collect()
  println(capacityAnalysis(array.map(_.get(0).toString.toDouble)))
  println(capacityAnalysis(array.map(_.get(1).toString.toDouble)))
  println(capacityAnalysis(array.map(_.get(2).toString.toDouble)))
  array = spark
    .sql(
      """
        | SELECT
        | mem_used*100.0/mem_total AS mem,
        | cpu_used*100.0/cpu_total AS cpu,
        | disk_used*100.0/disk_total AS disk
        | FROM tmp
        | WHERE node_id = 1
        | ORDER BY date
      """.stripMargin)
    .rdd.collect()
  println(capacityAnalysis(array.map(_.get(0).toString.toDouble)))
  println(capacityAnalysis(array.map(_.get(1).toString.toDouble)))
  println(capacityAnalysis(array.map(_.get(2).toString.toDouble)))

  array = spark
    .sql(
      """
        | SELECT
        | mem_used*100.0/mem_total AS mem,
        | cpu_used*100.0/cpu_total AS cpu,
        | disk_used*100.0/disk_total AS disk
        | FROM tmp
        | WHERE node_id = 2
        | ORDER BY date
      """.stripMargin)
    .rdd.collect()
  println(capacityAnalysis(array.map(_.get(0).toString.toDouble)))
  println(capacityAnalysis(array.map(_.get(1).toString.toDouble)))
  println(capacityAnalysis(array.map(_.get(2).toString.toDouble)))

  array = spark
    .sql(
      """
        | SELECT
        | mem_used*100.0/mem_total AS mem,
        | cpu_used*100.0/cpu_total AS cpu,
        | disk_used*100.0/disk_total AS disk
        | FROM tmp
        | WHERE node_id = 3
        | ORDER BY date
      """.stripMargin)
    .rdd.collect()
  println(capacityAnalysis(array.map(_.get(0).toString.toDouble)))
  println(capacityAnalysis(array.map(_.get(1).toString.toDouble)))
  println(capacityAnalysis(array.map(_.get(2).toString.toDouble)))
  //  spark.sql("select * from tmp").write.format("com.databricks.spark.csv").save("D:\\workspace\\pipeline-ml\\src\\test\\resources\\test1.csv")

}
