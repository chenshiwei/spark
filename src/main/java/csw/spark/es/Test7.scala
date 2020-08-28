package csw.spark.es


import java.util.Random

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
    val conf = new SparkConf().setAppName("Spark ES").setMaster("local[4]")

    val spark = SparkSession
        .builder().config(conf)
        .getOrCreate()
//    val random = new Random()
//    val array: Seq[Behaviour] = 0.until(30).map(i => {
//        val time = i > 100
//        Behaviour(time, s"10.1.240.${random.nextInt(255)}",
//            s"Action${random.nextInt(100)}"
//        )
//    })
//    val rdd: RDD[Behaviour] = spark.sparkContext.makeRDD(array)

    spark.read.format("org.elasticsearch.spark.sql")
                .options(Map("es.index.auto.create" -> "true", "es.nodes" -> "10.1.11.152:9200","es.net.http.auth.user"-> "admin",
                   "es.net.http.auth.pass"-> "Admin@123")).load("dw_heart_model/_doc").write.format("jdbc")
        .mode(SaveMode.Overwrite)
        .options(Map(
            "password"->"DBuser123!",
            "driver"->"com.mysql.jdbc.Driver",
            "dbtable"->"heart_model",
            "user"->"dbuser",
            "url"->"jdbc:mysql://10.1.11.151:3306/aiops_test"))
        .save()


    case class Behaviour(flag: Boolean,
                         ip: String,
                         command: String
                        )

}