package project1_logAnalysis

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext


case class Record(log_level: String, method: String, content: String)

object LogAnalysis {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR) //配置日志
    val sparkConf = new SparkConf()
      .setAppName("LogAnalysis")
      .setMaster("local[*]")
    //因为要用到spark sql , 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("LogAnalysis")
      .config(sparkConf)
      .getOrCreate()
    //利用sparkSession创建上下文
    val sc = spark.sparkContext
    //建立流式处理上下文  spark Streaming
    val ssc = new StreamingContext(sc, Seconds(2))

    //以上都表明在一个程序中，只能创建一个与spark的联接

    // Mysql配置
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "a")

    // 读入日志文件目录下的日志信息流
    val logStream = ssc.textFileStream("./logs/")

    // 从DStream中取出每个RDD, 将日志RDD信息流转换为dataframe
    logStream.foreachRDD((rdd: RDD[String]) => {
      import spark.implicits._
      val data = rdd.map(w => {
        val tokens = w.split("\t")
        Record(tokens(0), tokens(1), tokens(2))
      }).toDF()
      //println(  "本次RDD取到数据条数:"+data.count() )
      //data.show()
      //创建视图
      data.createOrReplaceTempView("alldata")

      // 条件筛选:只查看  error和 warn信息
      val logImp = spark.sql("select * from alldata where log_level='[error]'")
      logImp.show()
      // 输出到外部Mysql中
//      //利用 spark sql将结果保存到外部mysql中       DataFrame.read        DataFrame.write
      logImp.write.mode(SaveMode.Append)
        .jdbc("jdbc:mysql://localhost:3306/log_analysis", "important_logs", properties)
    })





    ssc.start()
    ssc.awaitTermination()
  }
}
