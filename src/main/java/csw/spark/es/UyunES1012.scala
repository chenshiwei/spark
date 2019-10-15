package csw.spark.es

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object UyunES1012 {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Spark ES").setMaster("local[4]")
        val spark = SparkSession
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

        spark.udf.register("window_timestamp", (time: Long, granularity: String) =>
            TimeUtils.leftGranularity(granularity)(time))

        spark.udf.register("TRANS_TO_MAP", new TransStructToMap(), DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))
        spark.udf.register("device",(tags:Map[String,String])=>tags("device"))
        spark.read.format("org.elasticsearch.spark.sql")
            .options(Map("es.index.auto.create" -> "true",
                "es.nodes" -> "10.1.61.238:19210"))
            .load("dw_zbmx_20190924/dw_zbmx")
            .createOrReplaceTempView("tmp")

//        "SELECT tenantId,window_timestamp(timestamp,'1min'),count(1) FROM tmp WHERE metric_name='system.disk.used' AND objectId='5d68c61cf1d4520e3c5e2538' GROUP BY tenantId,objectId,window_timestamp(timestamp,'1min')"
//            .show(10000000,false)
"SELECT *,device(TRANS_TO_MAP(tags)) FROM tmp WHERE tenantId='e10adc3949ba59abbe56e057f20f88dd' AND metric_name='system.disk.used'".show(1000000,false)
    }

}
