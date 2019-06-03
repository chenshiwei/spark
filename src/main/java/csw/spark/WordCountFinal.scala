package csw.spark

import org.apache.spark.{ SparkConf, SparkContext }

object WordCountFinal extends App {
  
  val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("WordCount"))
  
  sc.textFile("test").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).foreach(println)
  
}