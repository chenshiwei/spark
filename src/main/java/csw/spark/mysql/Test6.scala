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
object Test6 extends App {
    val DAY: Long = TimeUnit.DAYS.toMillis(1)
    val GTM_8_OFFSET: Long = TimeUnit.HOURS.toMillis(8)
    val conf = new SparkConf().setAppName("Spark mysql").setMaster("local[4]")

    val spark = SparkSession
        .builder().config(conf)
        .getOrCreate()

    def longToString(timestamp: Long, format: String): String =
        new SimpleDateFormat(format).format(new Date(timestamp))

    def leftDay(time: Long, timezoneOffset: Long = GTM_8_OFFSET): Long = {
        (time + timezoneOffset) / DAY * DAY - timezoneOffset
    }

    val random =new Random()
    val array: Seq[KPIS] =0.until(288).map(i=>{
    val time = leftDay(System.currentTimeMillis())+i*300000L
      KPIS(time,longToString(time,"yyyy-MM-dd HH:mm:ss"),
          random.nextDouble(),
          random.nextDouble(),
          random.nextDouble(),
          random.nextDouble(),
          random.nextDouble(),
          random.nextDouble(),
          random.nextDouble(),
          random.nextDouble()
      )
})
    val rdd: RDD[KPIS] = spark.sparkContext.makeRDD(array)

    spark.createDataFrame(rdd).write.format("jdbc")
        .mode(SaveMode.Append)
        .options(Map(
        "password"->"DBuser123!",
        "driver"->"com.mysql.jdbc.Driver",
        "dbtable"->"kpis_mock",
        "user"->"dbuser",
        "url"->"jdbc:mysql://10.1.50.56:3306/ai"))
            .save()

    case class KPIS(time:Long,
                    timestamp:String,
                    kpi1:Double,
                    kpi2:Double,
                    kpi3:Double,
                    kpi4:Double,
                    kpi5:Double,
                    kpi6:Double,
                    kpi7:Double,
                    kpi8:Double
                   )
}
