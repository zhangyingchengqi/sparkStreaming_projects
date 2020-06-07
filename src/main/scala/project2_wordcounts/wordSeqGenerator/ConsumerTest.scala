package project2_wordcounts.wordSeqGenerator

import java.util.concurrent._
import java.util.{Collections, Properties}
import kafka.utils.Logging
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import scala.collection.JavaConversions._

/*
用于测试(本身这个项目中使用的是spark streaming来完成消费端的功能的)
     请测试WordSeqProducer是否成功发送的消费者程序
 */

object ConsumerTest extends App { //继承自App，所以不用写 main方法 即这个object为程序入口
  val topic = "comments" //主题名
  val brokers = "localhost:9092,localhost:9093,localhost:9094" //kafka服务器地址
  val groupId = "yc74streaming" //组编号
  val example = new ConsumerTest(brokers, groupId, topic)
  example.run() //运行消费端
}


class ConsumerTest(val brokers: String,
                   val groupId: String,
                   val topic: String) extends Logging {

  val props = createConsumerConfig(brokers, groupId)
  val consumer = new KafkaConsumer[String, String](props)

  def shutdown() = {
    if (consumer != null)
      consumer.close();
  }
  //消费者的参数
  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true") //是否自动提交 消息偏移量
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000") //自动提交的时间间隔
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000") //会话超时时间
    //反序列化工具类
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def run() = {
    //订阅主题
    consumer.subscribe(Collections.singletonList(this.topic))
    //启动线程池   :  newSingleThreadExecutor一个单线程化的线程池，它只会用唯一的工作线程来执行任务，保证所有任务按照指定顺序(FIFO, LIFO, 优先级)执行
    //newCachedThreadPool:一个可缓存线程池，如果线程池长度超过处理需要，可灵活回收空闲线程，若无可回收，则新建线程
    //newFixedThreadPool: 一个定长线程池，可控制线程最大并发数，超出的线程会在队列中等待
    // newScheduledThreadPool: 一个定长线程池，支持定时及周期性任务执行
    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          //1秒拉取一次数据
          val records = consumer.poll(1000)

          for (record <- records) {
            System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset()+" at partition "+record.partition())
          }
        }
      }
    })
  }
}