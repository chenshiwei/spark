package csw.spark.mysql

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 作用:
  *
  * @author chensw
  * @since 2019/8/15
  */
object Test5 extends App {
    val conf = new SparkConf().setAppName("Spark mysql").setMaster("local[4]")

    val spark = SparkSession
        .builder().config(conf)
        .getOrCreate()
    val eventList = List(
        "机房1号湿度高报警",
        "机房2号湿度高报警",
        "机房3号湿度高报警",
        "机房4号湿度高报警",
        "机房5号湿度高报警",
        "机房1号温度高报警",
        "机房2号温度高报警",
        "机房3号温度高报警",
        "机房4号温度高报警",
        "机房5号温度高报警",
        "机房1号湿度低报警",
        "机房2号湿度低报警",
        "机房3号湿度低报警",
        "机房4号湿度低报警",
        "机房5号湿度低报警",
        "机房1号温度低报警",
        "机房2号温度低报警",
        "机房3号温度低报警",
        "机房4号温度低报警",
        "机房5号温度低报警",
        "机房1号UPS逆变输出报警",
        "机房2号UPS逆变输出报警",
        "机房3号UPS逆变输出报警",
        "机房4号UPS逆变输出报警",
        "机房5号UPS逆变输出报警",
        "机房1号漏水报警",
        "机房2号漏水报警",
        "机房3号漏水报警",
        "机房4号漏水报警",
        "机房5号漏水报警",
        "机房1号行间空调公共报警",
        "机房2号行间空调公共报警",
        "机房3号行间空调公共报警",
        "机房4号行间空调公共报警",
        "机房5号行间空调公共报警",
        "市电电量仪A相电压报警",
        "市电电量仪B相电压报警",
        "市电电量仪C相电压报警",
        "市电电量仪D相电压报警",
        "市电电量仪E相电压报警",
        "机房1号UPS旁路激活报警",
        "机房2号UPS旁路激活报警",
        "机房3号UPS旁路激活报警",
        "机房4号UPS旁路激活报警",
        "机房5号UPS旁路激活报警",
        "机房1号UPS设备状态报警",
        "机房2号UPS设备状态报警",
        "机房3号UPS设备状态报警",
        "机房4号UPS设备状态报警",
        "机房5号UPS设备状态报警",
        "机房1号UPS电量仪通讯失败",
        "机房2号UPS电量仪通讯失败",
        "机房3号UPS电量仪通讯失败",
        "机房4号UPS电量仪通讯失败",
        "机房5号UPS电量仪通讯失败",
        "机房1号行间空调高温报警",
        "机房2号行间空调高温报警",
        "机房3号行间空调高温报警",
        "机房4号行间空调高温报警",
        "机房5号行间空调高温报警",
        "机房1号UPS输入电压报警",
        "机房2号UPS输入电压报警",
        "机房3号UPS输入电压报警",
        "机房4号UPS输入电压报警",
        "机房5号UPS输入电压报警",
        "机房1号空调通讯失败",
        "机房2号空调通讯失败",
        "机房3号空调通讯失败",
        "机房4号空调通讯失败",
        "机房5号空调通讯失败",
        "机房1号UPS旁路超跟踪报警",
        "机房2号UPS旁路超跟踪报警",
        "机房3号UPS旁路超跟踪报警",
        "机房4号UPS旁路超跟踪报警",
        "机房5号UPS旁路超跟踪报警",
        "机房1号一号模块加湿器故障报警",
        "机房2号一号模块加湿器故障报警",
        "机房3号一号模块加湿器故障报警",
        "机房4号一号模块加湿器故障报警",
        "机房5号一号模块加湿器故障报警",
        "机房1号二号模块加湿器故障报警",
        "机房2号二号模块加湿器故障报警",
        "机房3号二号模块加湿器故障报警",
        "机房4号二号模块加湿器故障报警",
        "机房5号二号模块加湿器故障报警",
        "机房1号空调电源过欠压报警",
        "机房2号空调电源过欠压报警",
        "机房3号空调电源过欠压报警",
        "机房4号空调电源过欠压报警",
        "机房5号空调电源过欠压报警",
        "机房1号空调电源故障报警",
        "机房2号空调电源故障报警",
        "机房3号空调电源故障报警",
        "机房4号空调电源故障报警",
        "机房5号空调电源故障报警",
        "机房1号UPS电量仪停电或单相掉电报警",
        "机房2号UPS电量仪停电或单相掉电报警",
        "机房3号UPS电量仪停电或单相掉电报警",
        "机房4号UPS电量仪停电或单相掉电报警",
        "机房5号UPS电量仪停电或单相掉电报警"
    )

    println(eventList.length)
    val rdd: RDD[Event] = spark.read.option("header", true).csv("E:/tmp/test.csv")
        .rdd.map(row => {
        val time = row.getString(0).toLong + TimeUnit.DAYS.toMillis(42)
        Event(time, longToString(time, "yyyy-MM-dd HH:mm:ss"), eventList(row.getString(1).substring(5).toInt))
    })

    def longToString(timestamp: Long, format: String): String =
        new SimpleDateFormat(format).format(new Date(timestamp))
    spark.createDataFrame(rdd).write.format("jdbc")
        .mode(SaveMode.Append)
        .options(Map(
            "password" -> "DBuser123!",
            "driver" -> "com.mysql.jdbc.Driver",
            "dbtable" -> "event_mock2",
            "user" -> "dbuser",
            "url" -> "jdbc:mysql://10.1.50.56:3306/ai"))
        .save()

    case class Event(time: Long, timestamp: String, event: String)

}
