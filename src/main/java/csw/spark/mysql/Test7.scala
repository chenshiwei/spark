package csw.spark.mysql

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.{Date, Random}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 作用:
  *
  * @author chensw
  * @since 2019/8/15
  */
object Test7 extends App {
    val DAY: Long = TimeUnit.DAYS.toMillis(1)
    val GTM_8_OFFSET: Long = TimeUnit.HOURS.toMillis(8)
    val conf = new SparkConf().setAppName("Spark mysql").setMaster("local[4]")

    val spark = SparkSession
        .builder().config(conf)
        .getOrCreate()
    val random = new Random()
    def longToString(timestamp: Long, format: String): String =
        new SimpleDateFormat(format).format(new Date(timestamp))

    def leftDay(time: Long, timezoneOffset: Long = GTM_8_OFFSET): Long = {
        (time + timezoneOffset) / DAY * DAY - timezoneOffset
    }
    0.to(99).foreach(_ => {
        val array: Seq[Behaviour] = 0.until(288).map(i => {
            val time = leftDay(System.currentTimeMillis()) + i * 300000L + random.nextInt(300000)
            Behaviour(time, s"10.1.240.${random.nextInt(255)}",
                s"Action${random.nextInt(100)}"
            )
        })
        val rdd: RDD[Behaviour] = spark.sparkContext.makeRDD(array)



        spark.createDataFrame(rdd).write.format("jdbc")
            .mode(SaveMode.Append)
            .options(Map(
                "password" -> "DBuser123!",
                "driver" -> "com.mysql.jdbc.Driver",
                "dbtable" -> "user_behaviour",
                "user" -> "dbuser",
                "url" -> "jdbc:mysql://10.1.50.56:3306/ai"))
            .save()
    })

    case class Behaviour(timestamp: Long,
                         ip: String,
                         command: String
                        )

}
