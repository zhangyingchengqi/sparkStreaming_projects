package project1_logAnalysis

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream


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

    ssc.checkpoint("./chpoint")

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
    //  [级别]\t位置\t信息                                    ( 级别,1)
    val cached=logStream.map(_.split("\t")).map(   arr=>(arr(0),1)).cache()
    //有状态的操作
    val result=cached.updateStateByKey( updateFunc, new HashPartitioner(  ssc.sparkContext.defaultMinPartitions  ),  true )
    result.print()

    val r2=cached.reduceByKeyAndWindow( (a:Int,b:Int)=>a+b, Seconds(4)  , Seconds( 2) )
    //r2.print()
    printValues(  r2 )


    //r2.print()
    ssc.start()
    ssc.awaitTermination()
  }

  val updateFunc= (   iter:Iterator[ (String,Seq[Int] ,  Option[Int] ) ] ) =>{
    //方案一:当成一个三元组运算
    // iter.map(    t=> (  t._1,   t._2.sum+t._3.getOrElse(0)  )    )   //  ->  { word：总次数}
    //方案二: 模式匹配来实现
    iter.map{  case(x,y,z)=>(  x,  y.sum+z.getOrElse(0)   ) }
  }


  //定义一个打印函数，打印RDD中所有的元素
  def printValues(stream: DStream[(String, Int)]) {     //    DStream -> n个RDD组成  -> 一个RDD由n 条记录组成  -》一条记录由  (String, Int) 组成
    stream.foreachRDD(foreachFunc) //   不要用foreach()  ->  foreachRDD
    def foreachFunc = (rdd: RDD[(String, Int)]) => {
      val array = rdd.collect()   //采集 worker端的结果传到driver端.
      println("===============window窗口===============")
      for (res <- array) {
        println(res)
      }
      println("===============window窗口===============")

    }
  }


  /*
 结果分析:
     批处理时间间隔: 2秒
窗口长度: 4秒
滑动时间间隔: 2秒




-> 6个Error
state: 6个error
window: 6个error


->
state: 6个error
window: 6个error

-> 增加3个error
state: 6+3 个error
window:  3个error

   */
}
