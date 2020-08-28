package csw.spark.mongo

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 作用:
  *
  * @author chensw
  * @since 2019/11/18
  */
object MongoTest4 {


    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Spark mongo").setMaster("local[4]")
        val spark = SparkSession
            .builder().config(conf)
            .getOrCreate()


        spark.sparkContext.setLogLevel("warn")


        // 通过schema约束，直接获取需要的字段
        val df = spark.read.format("com.mongodb.spark.sql").options(Map(
            "uri"->"mongodb://mongo:MongoDB_863*^#@10.1.11.151:27017/admin",
            "database" -> "alert",
            "collection"->"ApplicationType"
        )).load()
        df.show()


        df.write.format("com.mongodb.spark.sql").options(Map(
            "uri"->"mongodb://10.1.11.151:27017/admin",
            "database" -> "alert",
            "collection"->"ApplicationType2"
        )).mode(SaveMode.Append).save()
        spark.stop()

    }
}
