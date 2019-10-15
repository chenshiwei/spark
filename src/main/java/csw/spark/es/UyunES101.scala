package csw.spark.es

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object UyunES101 {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Spark ES").setMaster("local[4]")
        //    conf.set("es.index.auto.create", "true")
        //    conf.set("es.nodes", "10.1.61.101")
        //    conf.set("es.port", "9200")

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

        spark.read.format("org.elasticsearch.spark.sql")
            .options(Map("es.index.auto.create" -> "true",
                "es.nodes" -> "10.1.240.117:19210"))
            .load("dm_deep_ec31d5f4026148a39c824ec9e138a368/dm_deep_ec31d5f4026148a39c824ec9e138a368")
            .createOrReplaceTempView("tmp")

//        1.to(4).map(i => spark.sql(s"SELECT time_str+60000*$i AS time_str,a,b,AVG_value_1 FROM tmp"))
//            .reduce(_.union(_)).write.format("org.elasticsearch.spark.sql")
//            .options(Map("es.index.auto.create" -> "true", "es.nodes" -> "10.1.240.113:19210"))
//            .mode(SaveMode.Append).save("dw_5fed05cfe35541bf8c9260a5ed96149b_20190826/dw_5fed05cfe35541bf8c9260a5ed96149b")
//        spark.sql(s"SELECT time_str+60000*60*24*12 AS time_str,a,b,AVG_value_1/2.0 as AVG_value_1 FROM tmp")
//            .write.format("org.elasticsearch.spark.sql")
//            .options(Map("es.index.auto.create" -> "true", "es.nodes" -> "10.1.240.113:19210"))
//            .mode(SaveMode.Append).save("dw_5fed05cfe35541bf8c9260a5ed96149b_20190824/dw_5fed05cfe35541bf8c9260a5ed96149b")
        //        s"""
        //           |SELECT window_timestamp(CAST(timestamp AS LONG),'1min') AS timestamp,
        //           |objectId,metric_name,
        //           |SUM(metric_value) AS SUM_metric_value_1
        //           | FROM tmp
        //           | GROUP BY window_timestamp(CAST(timestamp AS LONG),'1min'),objectId,metric_name
        //     """.stripMargin.write.format("org.elasticsearch.spark.sql")
        //            .options(Map("es.index.auto.create" -> "true",
        //                "es.nodes" -> "10.1.11.176:19210,10.1.11.177:19210",
        //                "es.port" -> "19210")).mode(SaveMode.Append).save("dw_5db16a26592243a7bcdd90f34a136a6a_20190815/dw_5db16a26592243a7bcdd90f34a136a6a")

                "SELECT * FROM tmp where timestamp=1571068320000".show(400,false)

        //      s"SELECT * FROM nbank_ebip".repartition(1).write.option("header", "true").option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
        //      .csv("F:/tmp/nbank")
        //      s"SELECT count(1) FROM nbank_ebip".write.format("org.elasticsearch.spark.sql")
        //         .options(Map(    "es.index.auto.create"-> "true",
        //      "es.nodes"-> "10.1.61.101",
        //      "es.port"->"9200")).save("spark_es/test")

        //"""
        //  |select metric_name,avg(cast(metric_value as double)) as sl from e10adc3949ba59abbe56e057f20f88dd_metric_datapoint_20181130_7 group by metric_name
        //""".stripMargin.show()
        //    Thread.sleep(100000)
        //        "SELECT *,cast(tenantId as string) as pp FROM tmp".createOrReplaceTempView("tmp")
        //        "SELECT tenantId FROM tmp where pp ='e10adc3949ba59abbe56e057f20f88dd' or  pp ='' ".show()

        //    spark.udf.register("strptime", (str: String) => stringToLong(str, "yyyy-MM-dd HH:mm:ss"))

        //    "SELECT *,strptime(occur_time) FROM spider_event ORDER BY occur_time".show(false)
        //      .repartition(1).write.option("header", "true").csv("E:/tmp/spider_event2")

        //    "SELECT `@COLLECT_IP`,userName,logTimestamp,type,TIME FROM e10adc3949ba59abbe56e057f20f88dd_linux_20180821_bs_system".show(1000,false)


    }

}
