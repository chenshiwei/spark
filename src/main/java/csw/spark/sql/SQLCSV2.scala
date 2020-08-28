package csw.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * @author chensw
  * @since at 2016-11-30下午3:04:46
  */
object SQLCSV2 {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder().master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()

//    1535811000000L.to(1536934200000L,300000L*12*24).map(URLEncoderTest=>
    //      spark.read.csv(s"D:\\tmp\\2019AIOps_data_(20190131update)\\$URLEncoderTest.csv"))
    //      .reduce(_.union(_)).createOrReplaceTempView("tmp")
    //
    //    spark.sql("SELECT _c0,_c1,_c2,_c3,_c4,avg(_c5) FROM tmp group by _c0,_c1,_c2,_c3,_c4")
    //      .repartition(1).write.csv("D:\\tmp\\csv3")

//    1535808300000L.to(1536931500000L,300000L*12*24).map(URLEncoderTest=>
//      spark.read.csv(s"D:\\tmp\\2019AIOps_data_(20190131update)\\$URLEncoderTest.csv"))
//      .reduce(_.union(_)).createOrReplaceTempView("tmp")
//
//    spark.sql("SELECT _c0,_c1,_c2,_c3,_c4,avg(_c5) FROM tmp group by _c0,_c1,_c2,_c3,_c4")
//      .repartition(1).write.csv("D:\\tmp\\csv3")
//    1535791500000L.to(1536914700000L,300000L*12*24).map(URLEncoderTest=>
//      spark.read.csv(s"D:\\tmp\\2019AIOps_data_(20190131update)\\$URLEncoderTest.csv"))
//      .reduce(_.union(_)).createOrReplaceTempView("tmp")
//
//    spark.sql("SELECT _c0,_c1,_c2,_c3,_c4,avg(_c5) FROM tmp group by _c0,_c1,_c2,_c3,_c4")
//      .repartition(1).write.csv("D:\\tmp\\csv3")
    1535782500000L.to(1536905700000L,300000L*12*24).map(l=>
      spark.read.csv(s"D:\\tmp\\2019AIOps_data_(20190131update)\\$l.csv"))
      .reduce(_.union(_)).createOrReplaceTempView("tmp")

    spark.sql("SELECT _c0,_c1,_c2,_c3,_c4,avg(_c5) FROM tmp group by _c0,_c1,_c2,_c3,_c4")
      .repartition(1).write.csv("D:\\tmp\\csv3")
  }
}
