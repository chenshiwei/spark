package csw.spark.mongo

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 作用:
  *
  * @author chensw
  * @since 2019/11/18
  */
object MongoTest3 {


    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Spark mongo").setMaster("local[4]")
        val spark = SparkSession
            .builder().config(conf)
            .getOrCreate()


        spark.sparkContext.setLogLevel("warn")

        val tb_test: DataFrame = MongoSpark.load(spark, ReadConfig(
            Map(
                "spark.mongodb.input.uri" -> "mongodb://root:Root_123@10.1.50.56:27017",
                "database" -> "alert",
                "collection" -> "ActionConfig"
            )))
        tb_test.show(false)

        MongoSpark.save(tb_test.drop("_id"),WriteConfig(   Map(
            "spark.mongodb.output.uri" -> "mongodb://root:Root_123@10.1.50.56:27017",
            "database" -> "alert",
            "collection" -> "ActionConfig3"
        )))
        spark.close()
    }
}
