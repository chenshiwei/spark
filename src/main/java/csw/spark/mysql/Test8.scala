package csw.spark.mysql

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

/**
  * 作用:
  *
  * @author chensw
  * @since 2019/8/15
  */
object Test8 extends App {
    val conf = new SparkConf().setAppName("Spark mysql").setMaster("local[4]")

    val spark = SparkSession
        .builder().config(conf)
        .getOrCreate()

    spark.read.format("jdbc")
        .options(Map(
            "password" -> "DBuser123!",
            "driver" -> "com.mysql.jdbc.Driver",
            "dbtable" -> "user_behaviour",
            "user" -> "dbuser",
            "url" -> "jdbc:mysql://10.1.50.56:3306/ai"))
        .load().createOrReplaceTempView("tmp")

    val documentDF = spark.sql("select floor(timestamp/3600000)*3600000 as timestamp,ip,collect_list(command) as commands from tmp group by floor(timestamp/3600000)*3600000,ip order by timestamp")
    //        .show(1000000,truncate = false)
    //        .createOrReplaceTempView("tmp")

    val word2Vec = new Word2Vec()
        .setInputCol("commands")
        .setOutputCol("result")
        .setVectorSize(5)
        .setMinCount(0)

    val model = word2Vec.fit(documentDF)

    val result = model.transform(documentDF)

    import org.apache.spark.ml.linalg.DenseVector

//    result.show(10000,false)
    result.select("result").rdd.foreach(row => println(row.getAs[DenseVector](0).values.mkString(",")))

}
