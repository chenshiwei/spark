package csw.spark.mysql

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

/**
  * 作用:
  *
  * @author chensw
  * @since 2018/10/26
  */
object Test4 extends App {
  val conf = new SparkConf().setAppName("Spark mysql").setMaster("local[4]")

  val spark = SparkSession
    .builder().config(conf)
    .getOrCreate()


  spark.read
    .format("jdbc").options(Map(
    "password" -> "123456",
    "driver" -> "com.mysql.jdbc.Driver",
    "dbtable" -> "shen_dan",
    "user" -> "root",
    "url" -> "jdbc:mysql://10.1.53.233:3306/ai")).load().createOrReplaceTempView("tmp")

  val df = spark.sql("SELECT * FROM tmp")

  val assembler1 = new VectorAssembler()
    .setInputCols(Array(/*"trial_volume","success", */ "total_amount", "backlog", "pre_input_volume"))
    .setOutputCol("features")

  val Array(trainingData, testData) = assembler1.transform(df).randomSplit(Array(0.9, 0.1),0)
  val rf = new LinearRegression()
    .setLabelCol("trial_volume")
    .setFeaturesCol("features")
//    .setMaxDepth(6)
//      .setNumTrees(15)
  rf.fit(trainingData).transform(testData).select("date_str","trial_volume", "prediction").show(100)
}
