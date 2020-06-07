package project2_wordcounts.wordSeqGenerator

import java.util.Properties
import scala.util.Random
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

/*
模拟类用于连接kafka,并向其推送数据.
后面可以用 pachong 来代替.
启动与关闭:
        1. 先启 kafka自带的 zk(端口2181): bin/zookeeper-server-start.sh config/zookeeper.properties 1>/dev/null 2>&1 &
        2. 再启动 kafka(端口9092):
              bin/kafka-server-start.sh config/server.properties  &
              bin/kafka-server-start.sh config/server-1.properties  &
              bin/kafka-server-start.sh config/server-2.properties  &

        3. 关闭:  sh kafka-server-stop.sh
                  sh zookeeper-server-stop.sh

   创建主题: bin/kafka-topics.sh --create --zookeeper localhost:2181  --replication-factor 3  --partitions 3 --topic  comments
   主题列表:  bin/kafka-topics.sh --list --zookeeper localhost:2181
   查看主题中消息详情: bin/kafka-topics.sh --describe --zookeeper localhost:2181    --topic comments
   发送消息: bin/kafka-console-producer.sh --broker-list localhost:9092,localhost:9093,localhost:9094 --topic comments
   消费消息:
     bin/kafka-console-consumer.sh --bootstrap-server localhost:9092,localhost:9093,localhost:9094  --topic   comments  --from-beginning
 */
object WordSeqProducer extends App {
  val events = 10 //生成评论条数
  val topic = "comments"   //主题名
  val brokers = "localhost:9092,localhost:9093,localhost:9094"  // kafka brokers

  val rnd = new Random()    //用于随机地生成文本。

  val props = new Properties()   //kafka producer参数
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "wordFreqGenerator")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)
  val t = System.currentTimeMillis()   // 获取系统当前时间，用于性能检测
  // 读取汉字字典
  val source = scala.io.Source.fromFile("data/hanzi.txt")
  val lines = try source.mkString finally source.close()
  //控制生成 events 条消息
  for (nEvents <- Range(0, events)) {
    // 生成模拟评论数据(user, comment)
    val sb = new StringBuilder()
    //每条消息最多200个字符
    for (ind <- Range(   0, rnd.nextInt(200)    )) {
      sb += lines.charAt(     rnd.nextInt(lines.length())    )
    }
    val userName = "user_" + rnd.nextInt(100)    // 主题   键        值
    val data = new ProducerRecord[String, String](topic, userName, sb.toString())
    producer.send(data)
  }
  System.out.println("每秒可以发送消息: " + events * 1000 / (System.currentTimeMillis() - t))
  producer.close()
}
