package csw.spark.csv

import org.apache.spark.sql.SaveMode

/**
  * 作用:
  *
  * @author chensw
  * @since 2020/3/17
  */
object Test2 extends App{
    spark.read.option("inferSchema", "true").option("header","true").csv("D:\\Documents\\2007\\monitor_test(1).csv")
        .createOrReplaceTempView("tmp")
    spark.sql("select *,to_unix_timestamp(timestamp, 'yyyy/MM/dd HH:mm')*1000 as timestamp1 from tmp").drop("timestamp")
        .createOrReplaceTempView("tmp")
    spark.sql("select *,timestamp1 timestamp from tmp").drop("timestamp1")
//        .createOrReplaceTempView("tmp")
        .write.format("org.elasticsearch.spark.sql")
        .options(Map("es.index.auto.create" -> "true", "es.nodes" -> "10.1.11.154:9200","es.net.http.auth.user"-> "admin",
            "es.net.http.auth.pass"-> "Admin@123"))
        .mode(SaveMode.Append).save("dw_monitor-test/_doc")
//        .printSchema()
//        .write.format("jdbc")
//        .mode(SaveMode.Append)
//        .options(Map(
//            "password" -> "DBuser123!",
//            "driver" -> "com.mysql.jdbc.Driver",
//            "dbtable" -> "AS01C1_CPU",
//            "user" -> "dbuser",
//            "url" -> "jdbc:mysql://10.1.11.151:3306/aiops_test"))
//        .save()
}
