package csw.spark.mysql

import java.text.SimpleDateFormat
import java.util.{Date, Random}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

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
    val random = new Random()

    def create(): KPI = {
        val time1: Long = 1566403200000L + 300000 * random.nextInt(576)
        KPI(time1,
            longToString(time1, "yyyy-MM-dd HH:mm:ss"),
            s"a${random.nextInt(10)}",
            s"b${random.nextInt(10)}",
            random.nextInt(100).toDouble)
    }

    def longToString(timestamp: Long, format: String): String =
        new SimpleDateFormat(format).format(new Date(timestamp))

    val array: Seq[KPI] = 0.until(100000).map(_ => create())


    //  spark.read.option("header", true).csv("F:\\mock2.csv")
    ////    .show(100)
    spark.createDataFrame(array)
//        .show(40)
        .write
//        spark.read
       .format("jdbc")
        .mode(SaveMode.Append)
        .options(Map(
          "password"->"DBuser123!",
          "driver"->"com.mysql.jdbc.Driver",
          "dbtable"->"kpi_mock",
          "user"->"dbuser",
          "url"->"jdbc:mysql://10.1.50.56:3306/ai"))
        .save()
//            .load().createOrReplaceTempView("tmp")

//    spark.sql("select time,sum(value) as value from tmp group by time order by time").show(100000)

    //  spark.sql("select * from tmp").write.format("com.databricks.spark.csv").save("D:\\workspace\\pipeline-ml\\src\\test\\resources\\test1.csv")

}

case class KPI(time: Long, time_str: String, a: String, b: String, value: Double)