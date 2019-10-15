package csw.spark.es

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.{Calendar, Date}

/**
  * 时间日期工具类
  *
  * @author chensw
  * @since 2016-3-31 下午3:11:08
  */
object TimeUtils {
    val SECOND: Long = TimeUnit.SECONDS.toMillis(1)
    val MINUTE: Long = TimeUnit.MINUTES.toMillis(1)
    val HOUR: Long = TimeUnit.HOURS.toMillis(1)
    val DAY: Long = TimeUnit.DAYS.toMillis(1)
    val GTM_8_OFFSET: Long = TimeUnit.HOURS.toMillis(8)

    def leftGranularity(granularity: String): Long => Long = {
        granularity match {
            case "1min" => TimeUtils.leftMinute
            case "5min" => TimeUtils.leftFiveMinute
            case "10min" => TimeUtils.leftTenMinute
            case "15min" => TimeUtils.leftFifteenMinute
            case "30min" => TimeUtils.leftThirtyMinute
            case "1h" | "1hour" => TimeUtils.leftHour
            case "3h" | "3hour" => TimeUtils.leftThreeHour(_, GTM_8_OFFSET)
            case "12h" | "12hour" => TimeUtils.leftTwelveHour(_, GTM_8_OFFSET)
            case "1d" | "1day" => TimeUtils.leftDay(_, GTM_8_OFFSET)
            case _ => time => time
        }
    }

    def leftMinute(time: Long): Long = {
        leftGap(time, MINUTE)
    }

    def leftGap(time: Long, gap: Long): Long = {
        time - time % gap
    }

    def leftFiveMinute(time: Long): Long = {
        leftGap(time, 5 * MINUTE)
    }

    def leftTenMinute(time: Long): Long = {
        leftGap(time, 10 * MINUTE)
    }

    def leftFifteenMinute(time: Long): Long = {
        leftGap(time, 15 * MINUTE)
    }

    def leftThirtyMinute(time: Long): Long = {
        leftGap(time, 30 * MINUTE)
    }

    def leftHour(time: Long): Long = {
        leftGap(time, HOUR)
    }

    def leftThreeHour(time: Long, timezoneOffset: Long = GTM_8_OFFSET): Long = {
        (time + timezoneOffset) / (3 * HOUR) * 3 * HOUR - timezoneOffset
    }

    def leftTwelveHour(time: Long, timezoneOffset: Long = GTM_8_OFFSET): Long = {
        (time + timezoneOffset) / (12 * HOUR) * 12 * HOUR - timezoneOffset
    }

    def leftDay(time: Long, timezoneOffset: Long = GTM_8_OFFSET): Long = {
        (time + timezoneOffset) / DAY * DAY - timezoneOffset
    }

    def getGranularity(granularity: String): Long = {
        granularity match {
            case "1min" => MINUTE
            case "5min" => 5 * MINUTE
            case "10min" => 10 * MINUTE
            case "15min" => 15 * MINUTE
            case "30min" => 30 * MINUTE
            case "1h" | "1hour" => HOUR
            case "3h" | "3hour" => 3 * HOUR
            case "12h" | "12hour" => 12 * HOUR
            case "1d" | "1day" => DAY
            case _ => 0
        }
    }

    def getZero(time: Long, timeUnits: Int*): Long = {
        val calendar: Calendar = Calendar.getInstance()
        calendar.setTimeInMillis(time)
        timeUnits.foreach(timeUnit => {
            calendar.set(timeUnit, 0)
        })
        calendar.getTimeZone.setID("GMT+08:00")
        calendar.getTimeInMillis
    }

    def leftSevenDay(time: Long, timezoneOffset: Long = GTM_8_OFFSET): Long = {
        val calendar: Calendar = Calendar.getInstance()
        calendar.setTimeInMillis(time)
        calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY)
        leftDay(calendar.getTimeInMillis, timezoneOffset)
    }

    def rightDay(time: Long, timezoneOffset: Long = GTM_8_OFFSET): Long = {
        math.ceil((time + timezoneOffset) / DAY.toDouble).toLong * DAY - timezoneOffset
    }

    def isWorkDay(timestamp: Long): Boolean = {
        val cal = Calendar.getInstance()
        cal.setTimeInMillis(timestamp)
        val week = cal.get(Calendar.DAY_OF_WEEK)
        week != Calendar.SATURDAY && week != Calendar.SUNDAY
    }

    def getWeek(timestamp: Long): Int = {
        val cal = Calendar.getInstance()
        cal.setTimeInMillis(timestamp)
        cal.get(Calendar.DAY_OF_WEEK) - 1

    }

    def stringToLong(strTime: String, format: String): Long =
        stringToDate(strTime, format).getTime

    def stringToDate(dateString: String, format: String): Date =
        new SimpleDateFormat(format).parse(dateString)

    def dateToString(date: Date, format: String): String =
        new SimpleDateFormat(format).format(date)

    def longToString(timestamp: Long, format: String): String =
        new SimpleDateFormat(format).format(new Date(timestamp))

}
