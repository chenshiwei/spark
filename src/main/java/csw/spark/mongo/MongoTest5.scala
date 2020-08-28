package csw.spark.mongo

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.matching.Regex

/**
  * 作用:
  *
  * @author chensw
  * @since 2019/11/18
  */
object MongoTest5 {

    def main(args: Array[String]): Unit = {
   val value = "mongodb://mongo:MongoDB_863*^#@10.1.11.151:27017/admin"
        val str = value.substring(value.indexOf("//"))
        println(str.split(":")(1).split("@")(0))
    }
}
