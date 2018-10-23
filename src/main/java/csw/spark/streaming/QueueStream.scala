package csw.spark.streaming

import java.util.concurrent.SynchronousQueue

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable


object QueueStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val ssc =new StreamingContext(conf,Seconds(3))
    val queue = new mutable.Queue[RDD[Int]]()
    val steam = ssc.queueStream(queue)
    val result = steam.map(_%10->1).reduceByKey(_+_)
    result.print()
    ssc.start()
    1.to(10).foreach(i=>{
      queue += ssc.sparkContext.makeRDD(i.to(100),3)
      Thread.sleep(1000)
    })
//    ssc.stop()
    ssc.awaitTermination()
  }

}
