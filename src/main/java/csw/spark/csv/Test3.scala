package csw.spark.csv

import org.apache.spark.sql.DataFrame

/**
  * 作用:
  *
  * @author chensw
  * @since 2020/3/17
  */
object Test3 extends App {
        spark.read.option("header", "true")
        .option("inferschema", "true").csv("E:\\tmp\\zpian.csv")
    .createOrReplaceTempView("tmp")
    val df: DataFrame = """SELECT '4eec344173550942b6f792bb' AS objectId,
      |cast(metric_value as double) AS metric_value,
      | 'zpian' AS metric_name,
      | 'e10adc3949ba59abbe56e057f20f88dd' AS tenantId,
      | 'e10adc3949ba59abbe56e057f20f88dd' AS uuid,
      | '' AS tags,
      | 'zpian' AS type,
      |cast(timestamp as long)*1000 AS timestamp,
      |cast(timestamp as long)*1000  AS ingress_time
      | from tmp""".stripMargin
    df.printSchema()
    df.show()

    df.write.format("org.elasticsearch.spark.sql")
        .options(Map("es.index.auto.create" -> "true",
            "es.nodes" -> "10.1.11.154:9200",
        "es.net.http.auth.user"->"admin",
        "es.net.http.auth.pass"->"Admin@123")).mode("append")
        .save("dw_metric_model_20200807/_doc")
}
