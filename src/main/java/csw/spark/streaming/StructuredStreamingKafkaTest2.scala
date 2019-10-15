package csw.spark.streaming

/**
  * 作用:
  *
  * @author chensw
  * @since 2019/10/12
  */


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}

object StructuredStreamingKafkaTest2 {
    def main(args: Array[String]): Unit = {
        //创建Spark SQL切入点
        val spark = SparkSession.builder().master("local[4]").appName("Structured").getOrCreate()
        //读取服务器端口发来的行数据，格式是DataFrame
        spark.conf.set("spark.sql.streaming.checkpointLocation", "D:/test")
        val df = spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "10.1.11.175:9292,10.1.11.176:9292,10.1.11.177:9292")
            .option("subscribe", "topic_access_9afcfa566f6e4b6c9ceb33a36419fe18")
            //            .option("subscribe", "kpis_3")
            .option("startingOffsets", "earliest")
            .load()
        //隐士转换（DataFrame转DataSet）
        import org.apache.spark.sql.functions._
        import spark.implicits._
        val smallBatchSchema = StructType(Seq(
            StructField("kpi1", DoubleType, nullable = true),
            StructField("kpi2", DoubleType, nullable = true),
            StructField("kpi3", DoubleType, nullable = true),
            StructField("kpi4", DoubleType, nullable = true),
            StructField("kpi5", DoubleType, nullable = true),
            StructField("kpi6", DoubleType, nullable = true),
            StructField("kpi7", DoubleType, nullable = true),
            StructField("kpi8", DoubleType, nullable = true),
            StructField("time", LongType, nullable = true)
        ))

        val query = df.selectExpr("CAST(value AS STRING)")
//            .select(from_json($"json", schema = smallBatchSchema).as("data"))
            //            .select(to_json($"data").as("value"))
//            .selectExpr("data.*")
            .writeStream
//            .format("kafka")
            //            .option("kafka.bootstrap.servers", "10.1.11.175:9292,10.1.11.176:9292,10.1.11.177:9292")
            //            .option("topic", "topic1")
            //            .start()
            .outputMode("update")
            .format("console")
            .start()

        query.awaitTermination()

    }
}
