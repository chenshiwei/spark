package csw.spark.mysql

import csw.spark.mysql.Demo.capacityAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.SparkSession

/**
  * 作用:
  *
  * @author chensw
  * @since 2018/10/26
  */
object Test3 extends App {
  val conf = new SparkConf().setAppName("Spark mysql").setMaster("local[4]")

  val spark = SparkSession
    .builder().config(conf)
    .getOrCreate()


  spark.read
    .format("jdbc").options(Map(
    "password" -> "123456",
    "driver" -> "com.mysql.jdbc.Driver",
    "dbtable" -> "mock2",
    "user" -> "root",
    "url" -> "jdbc:mysql://10.1.53.233:3306/dbgirl")).load().createOrReplaceTempView("tmp")

  val df = spark.sql("SELECT cast(kpi1 as double) as kpi1,cast(kpi2 as double) as kpi2,cast(kpi3 as double) as kpi3,cast(kpi4 as double) as kpi4,cast(kpi5 as double) as kpi5,cast(kpi6 as double) as kpi6,cast(kpi7 as double) as kpi7,cast(kpi8 as double) as kpi8,cast(kpi9 as double) as kpi9,cast(kpi10 as double) as kpi10,cast(kpi11 as double) as kpi11,cast(kpi12 as double) as kpi12,cast(kpi13 as double) as kpi13 FROM tmp")
  val assembler = new VectorAssembler()
    .setInputCols(Array("kpi1", "kpi2", "kpi3", "kpi4", "kpi5", "kpi6", "kpi7", "kpi8", "kpi9","kpi10", "kpi11", "kpi12"))
    .setOutputCol("features")

  df.show()
  val df2= assembler.transform(df)
  val Array(trainingData, testData) = df2.randomSplit(Array(0.8, 0.2))
  val rf = new RandomForestRegressor()
    .setLabelCol("kpi13")
    .setFeaturesCol("features")
  rf.fit(trainingData).transform(testData).select("kpi13","prediction").show(100)
}
