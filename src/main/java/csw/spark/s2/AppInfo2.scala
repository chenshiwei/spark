package csw.spark.s2

import org.apache.spark.sql.Row

case class AppInfo2(appid: String, platform: String, url: String) 

object AppInfo2 {
  def apply(row: Row): AppInfo2 = {
    new AppInfo2(row(1).toString(), row.getString(2), row.getString(3))
  }
}