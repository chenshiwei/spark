package csw.spark.csv

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._

/**
  * 作用:
  *
  * @author chensw
  * @since 2019/12/12
  */
object Test extends App {

    val df = 1.to(19).map(i => f"$i%3d".replaceAll(" ", "0")).map { index =>
        spark.read.option("header", "true")
            .option("inferschema", "true").csv(s"F:\\数据\\FEATURES-2014-2015\\part-$index.csv")
    }.reduce(_.union(_))
    df.createOrReplaceTempView("tmp")
    var i = 0
    val list = df.schema.map(f => {
        i += 1
        val name= "C_" + f"$i%3d".replaceAll(" ", "0")+"_"+ f.name.split(":")(0).trim.replaceAll(" ", "_").replaceAll("\\.","")
        StructField(name, if(i==3)LongType else if(i==4) StringType else f.dataType, nullable = true)
    }).toList
//    val list = df.schema.map(f => {
//        i += 1
//
//       val name= "C_" + f"$i%3d".replaceAll(" ", "0")+"_"+ f.name.split(":")(0).trim.replaceAll(" ", "_").replaceAll("\\.","")
//        StructField(name, f.dataType, nullable = true)
//    }).toList
    val rdd="SELECT host host_1,process process_2,unix_timestamp(timestamp, 'yyyy-MM-dd HH:mm')*1000 as timestamp_3,cast(isAnomaly as string) as isAnomaly_4,* from tmp"
        .drop("host","process","timestamp","isAnomaly").rdd
        val schema = StructType(list)

        spark.createDataFrame(rdd, schema)
//           .write.format("jdbc")
//            .mode(SaveMode.Append)
//                        .options(Map(
//                "password" -> "DBuser123!",
//                "driver" -> "com.mysql.jdbc.Driver",
//                "dbtable" -> "features_2014_2015",
//                "user" -> "dbuser",
//                "url" -> "jdbc:mysql://10.1.240.109:3306/uyun_udap"))
//            .save()
            .write.format("org.elasticsearch.spark.sql")
            .options(Map("es.index.auto.create" -> "true", "es.nodes" -> "10.1.50.240:19210"))
            .mode(SaveMode.Append).save("dw_features_2014_2015_7501147_20191218/dw_features_2014_2015_7501147")
//            .createOrReplaceTempView("tmp")
    //    "SELECT * FROM tmp".schema.map(_.name).foreach(println)
}