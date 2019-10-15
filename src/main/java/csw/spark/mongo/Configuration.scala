package csw.spark.mongo

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * spark定时任务需要的参数配置
  * 主要配置一个静态的spark:SparkSession
  * R11后主程序弃用,用uyun.octopus.compute.scheduler.SchedulerConfig代替,
  * Test和小程序依然使用此类,如数据重跑,清理等
  *
  * @author chensw
  * @since at 2016-4-14下午3:04:46
  */

object Configuration {

  private var sparkConf: SparkConf = _
  private var spark: SparkSession = _
  private var properties: Properties = _

  private var keySpace: String = _
  private var endTime: Long = _

  def getProperties: Properties = {
    if (properties == null) {
      getSparkConf()
      properties
    } else properties
  }

  def getEndTime: Long = this.endTime

  def setEndTime(endTime: Long): Unit = this.endTime = endTime

  def getKeySpace: String = {
    if (keySpace == null) getSparkConf.get("cassandra.keyspace", "uem_octopus")
    else keySpace
  }

  def setKeySpace(keySpace: String): Unit = this.keySpace = keySpace

  def getSparkConf: SparkConf = if (sparkConf == null) getSparkConf() else this.sparkConf

  /**
    * 首次启动调用配置文件，将所有需要的配置加载到sparkConf中
    */
  def getSparkConf(path: String = "src/main/resources/spark.properties"): SparkConf = synchronized {
    if (sparkConf == null) {
      properties = new Properties()
      properties.load(Source.fromFile(path).bufferedReader())
      import scala.collection.JavaConverters._
      sparkConf = new SparkConf().setAll(properties.asScala.map(conf => (conf._1, conf._2)))
    }
    this.keySpace = sparkConf.get("cassandra.keyspace", "uem_octopus")
    this.sparkConf
  }

  /**
    * 获取配置好的sc(SparkContext)
    */
  def getSc: SparkContext = getSparkSession.sparkContext

  def getSparkSession: SparkSession = {
    if (spark == null) {
      spark = SparkSession.builder().config(getSparkConf).getOrCreate()
    }
    spark
  }

  /**
    * 加载spark,redis,cassandra等配置
    *
    * @param reload :是否重新加载
    * @return
    */
  def load(register: Boolean = true, reload: Boolean = false): Unit = {

    // 0: 是否重新加载配置
    if (reload && spark != null) {
      closeAll()
    }

    // 1: 读入配置文件,并将所有的配置赋值到sparkConf
    sparkConf = getSparkConf

    // 5 创建SparkSession
    spark = SparkSession.builder().config(sparkConf).getOrCreate()
  }

  def closeAll(): Unit = {
    spark.stop()
    sparkConf = null
    properties.clear()
  }

}