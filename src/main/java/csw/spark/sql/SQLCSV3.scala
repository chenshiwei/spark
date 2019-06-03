package csw.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * @author chensw
  * @since at 2016-11-30下午3:04:46
  */
object SQLCSV3 {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder().master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()
    0.to(4).foreach(i => {
      spark.read.csv("D:\\tmp\\part-1.csv").createOrReplaceTempView("tmp1")
      spark.read.csv(s"D:\\tmp\\1538659500000.csv").createOrReplaceTempView("tmp2")


      //      spark.sql("SELECT sum(_c5) FROM tmp1").show()
      //      spark.sql("SELECT sum(_c5) FROM tmp2").show()
      val col = s"_c$i"
      spark.sql(s"select $col,sum(_c5) as _c5 from tmp1 group by $col order by $col")
        .createOrReplaceTempView("tmp1")
      spark.sql(s"select $col,sum(_c5) as _c5 from tmp2 group by $col order by $col")
        .createOrReplaceTempView("tmp2")
      spark.sql("select * from tmp1 where _c5>2250").createOrReplaceTempView("tmp1")
      //    spark.sql("select * from tmp2 where _c5>2000").createOrReplaceTempView("tmp2")
      spark.sql(s"select tmp1.$col,cast(tmp1._c5 as int) as _c51,cast(tmp2._c5 as int) as _c52,cast(tmp2._c5-tmp1._c5 as int),(tmp2._c5-tmp1._c5)/1510423.0,(tmp2._c5-tmp1._c5)/tmp1._c5 from tmp1,tmp2 where tmp1.$col = tmp2.$col")
        .repartition(1).write.csv(s"D:/tmp/$col")
    })
    //    spark.sql("select sum(_c5) from tmp1 where _c3='p23' and _c4='l4' and _c1='e08'").show(1000)
    //    spark.sql("select sum(_c5) from tmp2 where _c3='p23' and _c4='l4' and _c1='e08'").show(1000)
  }
}
