package project2_wordcounts.wordSeqGenerator

import java.util.concurrent._
import java.util.{Collections, Properties}
import kafka.utils.Logging
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import project2_wordcounts.wordSeqGenerator.ConsumerTest.args

import scala.collection.JavaConversions._

/*
用于测试   WordSeqProducer是否成功发送的消费者程序
 */
object ConsumerTest extends App {
  val topic = "comments"
  val brokers = "localhost:9092,localhost:9093,localhost:9094"
  val groupId="yc74streaming"
  val example = new ConsumerTest(  brokers, groupId, topic)
  example.run()
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

  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def run() = {
    consumer.subscribe(Collections.singletonList(this.topic))
    //启动线程池
    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          val records = consumer.poll(1000)

          for (record <- records) {
            System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
          }
        }
      }
    })
  }
}
