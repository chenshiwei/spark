package csw.spark.streaming

/**
  * 作用:
  *
  * @author chensw
  * @since 2019/10/12
  */


import org.apache.spark.sql.{DataFrame, SparkSession}

object StructuredStreamingTest {
    def main(args: Array[String]): Unit = {
        //创建Spark SQL切入点
        val spark = SparkSession.builder().master("local[4]").appName("Structured").getOrCreate()
        //读取服务器端口发来的行数据，格式是DataFrame
        val linesDF = spark.readStream.format("socket").option("host", "10.1.50.240")
            .option("port", 9999).load()
        //隐士转换（DataFrame转DataSet）
        import spark.implicits._
        //行转换成一个个单词
        val words = linesDF.as[String].flatMap(_.split(" "))
        //按单词计算词频
        val wordCounts: DataFrame = words.groupBy("value").count()

        val query = wordCounts.writeStream
            .outputMode("complete")
            .format("console")
            .start()

        query.awaitTermination()

    }
}
