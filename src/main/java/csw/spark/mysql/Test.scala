package csw.spark.mysql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 作用:
  *
  * @author chensw
  * @since 2018/10/26
  */
object Test extends App {
  val conf = new SparkConf().setAppName("Spark mysql").setMaster("local[4]")

  val spark = SparkSession
    .builder().config(conf)
    .getOrCreate()

  spark.read.option("header", true).csv("F:\\mock2.csv")
//    .show(100)
    .write
  //  spark.read
   .format("jdbc").options(Map(
      "password"->"123456",
      "driver"->"com.mysql.jdbc.Driver",
      "dbtable"->"shen_dan",
      "user"->"root",
      "url"->"jdbc:mysql://10.1.53.233:3306/ai")).save()

  //  spark.sql("select * from tmp").write.format("com.databricks.spark.csv").save("D:\\workspace\\pipeline-ml\\src\\test\\resources\\test1.csv")

}
