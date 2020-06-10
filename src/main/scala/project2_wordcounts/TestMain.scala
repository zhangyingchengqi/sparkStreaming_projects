package project2_wordcounts

import scala.collection.mutable.Map
import scala.collection.mutable.HashSet
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkConf
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import project2_wordcounts.streamingWC.service.{MysqlService, SegmentService}
import project2_wordcounts.streamingWC.utils.{BroadcastWrapper, Conf}
import spray.json._

//主程序类
object TestMain extends Serializable {

  @transient lazy val log = LogManager.getRootLogger

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR) //配置日志
    val sparkConf = new SparkConf().setAppName("WordFreqConsumer")
      .setMaster(Conf.master)
      //.set("spark.executor.memory", Conf.executorMem)
      //.set("spark.cores.max", Conf.coresMax)
      //.set("spark.local.dir", Conf.localDir)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.set("spark.streaming.kafka.maxRatePerPartition", Conf.perMaxRate) //设定对目标topic每个partition每秒钟拉取的数据条数
    val ssc = new StreamingContext(sparkConf, Seconds(Conf.interval))
    ssc.checkpoint("./chpoint") // window函数和有状态操作一定要设置
    // 创建直联
    // auto.offset.reset 值:
    //          earliest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
    //          latest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
    //          none: topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
    val topicsSet = Conf.topics.split(",").toSet
    val kafkaParams = scala.collection.immutable.Map[String, Object]("bootstrap.servers" -> Conf.brokers,
             "key.deserializer" -> classOf[StringDeserializer],
             "value.deserializer" -> classOf[StringDeserializer],
             "auto.offset.reset" -> "latest",
              "group.id" -> Conf.group,
              "enable.auto.commit" -> (true: java.lang.Boolean))
    /*
    kafka本地策略（  LocationStrategies ):
    1. PreferConsistent : 在可用的 executors 上均匀分布分区
    2. PreferBrokers: 如果 executor 与 Kafka 的代理节点在同一台物理机上，使用 PreferBrokers，会更倾向于在该节点上安排 KafkaLeader 对应的分区,以减少数据的网络传输。
    3. PreferFixed： 如果发生分区之间数据负载倾斜，使用 PreferFixed。可以指定分区和主机之间的映射（任何未指定的分区将使用相同的位置）
     */
    //    Subscribe:主题名固定,   SubscribePattern:主题名由正则表示    ,   Assign:固定主题和分区
    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )
    log.warn(s"初始化联接完成***>>>topic:${Conf.topics}   group:${Conf.group} localDir:${Conf.localDir} brokers:${Conf.brokers}")

    kafkaDirectStream.cache     ///缓存
    //kafkaDirectStream.print()
    //读取  用户词典库，加载成广播变量
    val words = BroadcastWrapper[(Long, HashSet[String])](ssc, (System.currentTimeMillis, MysqlService.getUserWords))

    //经过分词得到新的stream,               _指的是  kafka中的 (k,v)
    //        repartition:重分区
    val segmentedStream = kafkaDirectStream.map(_.value).repartition(10).transform(rdd => {
      if (System.currentTimeMillis - words.value._1 > Conf.updateFreq) {   //   更新频率  300000 //5min
        words.update((System.currentTimeMillis, MysqlService.getUserWords), true)
        log.warn("[BroadcastWrapper] 用户词典中单词更新了 ")
      }
      //   rdd中是消息( 语句)        调用分词服务，并传递  待争分的语句及用户词典
      rdd.flatMap(record => SegmentService.mapSegment(record, words.value._2))
    })

    //  ( word, 1 )

    //以entity_timestamp_beeword为key,统计本batch内各个key的计数
    val countedStream = segmentedStream.reduceByKey(_ + _)

    // countedStream  (单词，总和)
    countedStream.foreachRDD(MysqlService.save(  _  ))

    ssc.start()
    ssc.awaitTermination()
  }
}
