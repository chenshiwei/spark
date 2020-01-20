package csw.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 作用:
  *
  * @author chensw
  * @since 2019/12/12
  */
package object csv {
    val conf: SparkConf = new SparkConf().setAppName("Spark csv").setMaster("local[4]")

    val spark: SparkSession = SparkSession
        .builder().config(conf)
        .getOrCreate()

    implicit def convertToDF(sql: String): DataFrame = {
        try {
            spark.sql(sql)
        } catch {
            case e: Throwable =>
                println(e.getMessage)
                null
        }
    }
}
