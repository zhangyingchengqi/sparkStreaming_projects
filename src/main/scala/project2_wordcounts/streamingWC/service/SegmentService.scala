package project2_wordcounts.streamingWC.service

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import org.apache.log4j.{Level, LogManager, Logger}
import project2_wordcounts.streamingWC.utils.Conf
import scalaj.http._
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.collection.mutable.HashSet
import scala.collection.mutable.Map

/*
访问分词服务的业务类
 */
object SegmentService extends Serializable {
  @transient lazy val log = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ALL) //配置日志
    //单次请求测试
    //val segments=SegmentService.segment("http://localhost:8282/token/","今天是星期天,今天天气是不错")
    //print(  segments )
    //如有网络抖动的情况下，重新联接三次.
    //val segments = SegmentService.retry(3)(SegmentService.segment("http://localhost:8282/token/", "今天是星期天,今天天气是不错"))
    //print(segments)
    //3. 对拆分出来的词在用户词典表中 查找，并计数
    val record="今天是星期天,今天天气是不错"
    val wordDic=new HashSet[String]()
    wordDic.add( "今天")
    wordDic.add("星期天")
    val map=mapSegment(   record, wordDic)
    print( map )
  }

  /**
   * 将sentence发给分词服务器，获取返回的切分好的中文词汇，然后再到 userDict 查是否有这个词，如果有，则统计数据量.
   * 在Dstream中调用，传入记录内容和词典，通过retry(3)(segment(postUrl, record))实现失败重试3次方案，根据分词结果和词典，进行词典指定词的词频统计，并以Map[Word,count]返回
   *
   * @return
   */
  def mapSegment(record: String, wordDic: HashSet[String]): Map[String, Int] = {
    val preTime = System.currentTimeMillis
    val keyCount = Map[String, Int]()
    if (record == "" || record.isEmpty()) {
      log.warn(s"待切分语句为空: ${record}")
      keyCount
    } else {
      val postUrl = Conf.segmentorHost + "/token/"
      try {
        val wordsSet = retry(3)(segment(postUrl, record)) // 失败重试3次
        log.info(s"[拆分成功] 记录: ${record}\t耗时: ${System.currentTimeMillis - preTime}")
        // 进行词语统计
        //val keyCount = Map[String, Int]()
        for (word <- wordDic) {
          if (wordsSet.contains(word))
            keyCount += word -> 1
        }
        log.info(s"[keyCountSuccess] words size: ${wordDic.size} (entitId_createTime_word_language, 1):\n${keyCount.mkString("\n")}")
        keyCount
      } catch {
        case e: Exception => {
          log.error(s"[mapSegmentApiError] mapSegment error\tpostUrl: ${postUrl}${record}", e)
          keyCount
        }
      }
    }
  }

  /*
      根据传入的 url和 content发送请求，并解析返回的结果，将分词结果以HashSet形式返回
      参数: url:  http://localhost:8282/token/
            content:   今天是星期天
      返回值:   从jsonr的terms中取,按" "切分，形成数组 HashSet   ->  scala集合分两类: mutable,immutable
                HashSet: mutable
   */
  def segment(url: String, content: String): HashSet[String] = {
    val timer = System.currentTimeMillis()
    //地址栏的参数编码
    val c = URLEncoder.encode(content, StandardCharsets.UTF_8.toString)
    log.info("发送的请求为:" + url + "\t" + content + "\t" + c)
    //var response = Http(url + content).header("Charset", "UTF-8").charset("UTF-8").asString   //发送请求，得到响应
    var response = Http(url + c).header("Charset", "UTF-8").charset("UTF-8").asString //发送请求，得到响应
    log.info("响应为:" + response.code + "\t内容:" + response.body.toString)

    val dur = System.currentTimeMillis() - timer
    if (dur > 20) // 输出耗时较长的请求
      log.warn(s"[longVisit]>>>>>> api: ${url}${content}\ttimer: ${dur}")

    val words = HashSet[String]()
    response.code match { //匹配响应码
      case 200 => {
        //获取响应的结果，进行匹配
        response.body.parseJson.asJsObject.getFields("ret", "msg", "terms") match {
          case Seq(JsNumber(ret), JsString(msg), JsString(terms)) => {
            if (ret.toInt != 0) { //解析结果为空.
              log.error(s"[segmentRetError] vist api: ${url}?content=${content}\tsegment error: ${msg}")
              words
            } else {
              //解析到了 单词组，则切分
              val tokens = terms.split(" ")   // Array
              tokens.foreach(token => {
                words += token
              })
              words
            }
          }
          case _ => words
        }
      }
      case _ => { //响应码为其它则异常,返回空words
        log.error(s"[segmentResponseError] vist api: ${url}?content=${content}\tresponse code: ${response.code}")
        // [segmentResponseError] vist api: http://localhost:8282/content=xxxx\tresponse code: 500
        words
      }
    }
  }

  /**
   * 重试函数:  实现对函数 fn的n次重复调用。 在http请求时，会出现网络错误，这样这个函数就可以按要求的次数重新发送请求，如超出n次，则抛出异常
   *
   * @param n
   * @param fn
   * @return
   */
  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): T = {
    /*
    scala.util.Try 的结构与 Either 相似，Try 是一个 sealed 抽象类，具有两个子类，分别是 Succuss(x) 和 Failure(exception)。  模式匹配(  Option: Some(x)/None )
    Succuss会保存正常的返回值。 Failure 总是保存 Throwable 类型的值。
     */
    util.Try {
      fn
    } match { //利用scala中的Try函数:
      case util.Success(x) => {
        log.info(s"第${4-n}次请求")
        x
      }
      case _ if n > 1 => {   // _ 代表任意类型及任意值
        log.warn(s"[重试第 ${4 - n}次]")
        retry(n - 1)(fn)     // 递归
      }
      case util.Failure(e) => {
        log.error(s"[segError] 尝试调用API失败了三次", e)
        throw e
      }
    }
  }
}
