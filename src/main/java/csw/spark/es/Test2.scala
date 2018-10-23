package csw.spark.es

import java.text.SimpleDateFormat
import java.util.Date

object Test2 {

  def main(args: Array[String]): Unit = {
    val t = System.currentTimeMillis()
//    for (_ <- 1.to(100)) {
      println(stringToLong("2018-07-06 12:35:46", "yyyy-MM-dd HH:mm:spark"))
//    }
    println(System.currentTimeMillis() - t)
  }

  def stringToLong(strTime: String, format: String): Long =
    stringToDate(strTime, format).getTime

  def stringToDate(dateString: String, format: String): Date =
    new SimpleDateFormat(format).parse(dateString)

}
