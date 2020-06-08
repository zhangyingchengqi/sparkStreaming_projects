package project2_wordcounts.streamingWC.utils

import org.joda.time.{ DateTimeZone, _ }

/*
时间转换工具类
 */
object TimeParse extends Serializable {
  /*
  将字符串类型的时间戳转为 指定时间格式字符串
   */
  def timeStamp2String(timeStamp: String, format: String): String = {
    val ts = timeStamp.toLong * 1000;
    new DateTime(ts).toDateTime.toString(format)
  }

  /*
  将Long类型的时间戳转为 指定时间格式字符串
   */
  def timeStamp2String(timeStamp: Long, format: String): String = {
    new DateTime(timeStamp).toDateTime.toString(format)
  }
}
