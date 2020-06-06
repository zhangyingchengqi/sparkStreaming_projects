package project2_wordcounts.streamingWC.utils

/*
系统的配置
 */
object Conf {

  val nGram = 3    //分词器单位长度的参数
  val updateFreq = 300000 //5min

  // 分词服务 api
  val segmentorHost = "http://localhost:8282"

  // spark 参数
  val master = "local[*]"
  val localDir = "//Users/yingzhang/Desktop/data/tmp"
  val perMaxRate = "5"
  val interval = 3 // seconds
  val parallelNum = "15"
  val executorMem = "1G"
  val concurrentJobs = "5"
  val coresMax = "3"

  // kafka configuration
  val brokers = "localhost:9092,localhost:9093,localhost:9094"
  val zk = "localhost:2181"
  val group = "wordFreqGroup"
  val topics = "wcTopics"

  // mysql configuration
  val mysqlConfig = Map("url" -> "jdbc:mysql://localhost:3306/word_freq?characterEncoding=UTF-8", "username" -> "root", "password" -> "a")
  val maxPoolSize = 5
  val minPoolSize = 2
}
