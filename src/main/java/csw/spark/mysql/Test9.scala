package csw.spark.mysql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 作用:
  *
  * @author chensw
  * @since 2020/3/12
  */
object Test9 extends App{
    val conf = new SparkConf().setAppName("Spark mysql").setMaster("local[4]")

    val spark = SparkSession
        .builder().config(conf)
        .getOrCreate()

    spark.read.format("jdbc")
        .options(Map(
            "password" -> "DBuser123!",
            "driver" -> "com.mysql.jdbc.Driver",
            "dbtable" -> "hai_guan",
            "user" -> "dbuser",
            "url" -> "jdbc:mysql://10.1.11.52:3306/ai"))
        .load().createOrReplaceTempView("tmp")

    spark.sql("SELECT * FROM tmp").write.option("header",value = true).mode(SaveMode.Overwrite).csv("E:/tmp/hai_guan")
}
