package csw.spark.json

import csw.spark.es.TransStructToMap
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes

/**
  * 作用:
  *
  * @author chensw
  * @since 2018/10/25
  */
object JsonTest2 extends App {
  val conf = new SparkConf().setAppName("Spark Json").setMaster("local[4]")

  val spark = SparkSession
    .builder().config(conf)
    .getOrCreate()
  spark.udf.register("TRANS_TO_MAP", new TransStructToMap(), DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))
  spark.udf.register("after",(tags:Map[String,String])=>tags("after"))
  spark.read.format("json").load("F:\\数据\\u2jiankong-2019.12.json").createOrReplaceTempView("tmp")

spark.sql("SELECT after(TRANS_TO_MAP(_source)) FROM tmp")
.show(false)
}
