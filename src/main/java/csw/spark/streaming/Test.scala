package csw.spark.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val ssc =new StreamingContext(conf,Seconds(5))
    val lines: DStream[String] = ssc.textFileStream("file:///E:/tmp")
    val wc: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map(_->1).reduceByKey(_+_)
  wc.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
