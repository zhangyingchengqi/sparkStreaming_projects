package project2_wordcounts.streamingWC.service

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import project2_wordcounts.streamingWC.dao.MysqlManager
import project2_wordcounts.streamingWC.utils.TimeParse
import scala.collection.mutable

/**
 * 对数据库的存取操作。
     1) 查询user_words表，取出所有用户词典
     2) 统计 用户词典表中的词汇出现的次数，按月生成表格保存 。
 */
object MysqlService extends Serializable {

  @transient lazy val log = LogManager.getLogger(this.getClass)

  /*
  DStream  -> n 个RDD   -> 应用函数  切分 sentends  -> 调用分词  -》  RDD[    (String,1 )  ]     ->  reduce   ->  (String, count)  ->   调用   service来存
   */
  //关于数据库的操作，请参考:
  //  http://spark.apache.org/docs/latest/streaming-programming-guide.html#output-operations-on-dstreams
  // 中关于  foreachRDD 设计模式
  def save(rdd: RDD[(String, Int)]) = {
    if (!rdd.isEmpty) {
      //按分区循环RDD, 这样一个分区的所有数据只要一个Connection操作即可.
      rdd.foreachPartition(partitionRecords => {
        val preTime = System.currentTimeMillis
        //从连接池中获取一个连接
        val conn = MysqlManager.getMysqlManager.getConnection
        val statement = conn.createStatement
        try {
          conn.setAutoCommit(false)
          partitionRecords.foreach(record => {
            log.info("待操作的记录>>>>>>>" + record)
            val createTime = System.currentTimeMillis()  //系统时间的hao秒
            //按月建立一张新表存储数据:   按单词和时间进行区分，如单词和时间相同，则update, 不同则插入。
            var sql = s"CREATE TABLE if not exists `word_count_${TimeParse.timeStamp2String(createTime, "yyyyMM")}`(`id` int(11) NOT NULL AUTO_INCREMENT,`word` varchar(64) NOT NULL,`count` int(11) DEFAULT '0',`date` date NOT NULL, PRIMARY KEY (`id`), UNIQUE KEY `word` (`word`,`date`) ) ENGINE=InnoDB  DEFAULT CHARSET=utf8;"
            statement.addBatch(sql)   //将sql语句添加到批
            sql = s"insert into word_count_${TimeParse.timeStamp2String(createTime, "yyyyMM")} (word, count, date) " +
              s"values ('${record._1}',${record._2},'${TimeParse.timeStamp2String(createTime, "yyyy-MM-dd")}') " +
              s"on duplicate key update count=count+values(count) "      //   在更新表格中每个词的统计值时，用 on duplicate key进行词频数量的累加.
            log.info(   "sql:"+ sql )
            //   word列为  unique列
              // 如果在INSERT语句末尾指定了ON DUPLICATE KEY UPDATE，并且插入行后会导致在一个UNIQUE索引或PRIMARY KEY中出现重复值，则执行旧行UPDATE；如果不会导致唯一值列重复的问题，则插入新行
            statement.addBatch(sql)
            log.warn(s"[记录添加的批处理操作成功] record: ${record._1}, ${record._2}")

          })
          statement.executeBatch    //执行批处理   -> 当一个 RDD中的数据量太大   batch存不下  mysql 缓存存不下，调整batch的批的大小
          conn.commit
          log.warn(s"[保存的批处理操作完成] 耗时: ${System.currentTimeMillis - preTime}")
        } catch {
          case e: Exception =>
            log.error("[保存的批处理操作失败]", e)
        } finally {
          conn.setAutoCommit(true)
          statement.close()
          conn.close()
        }
      })
    }
  }

  /**
   * 加载用户词典
   */
  def getUserWords(): mutable.HashSet[String] = {
    val preTime = System.currentTimeMillis
    val sql = "select distinct(word) from user_words"    //distinct 去重
    val conn = MysqlManager.getMysqlManager.getConnection
    val statement = conn.createStatement
    try {
      val rs = statement.executeQuery(sql)
      val words = mutable.HashSet[String]()
      while (rs.next) {
        words += rs.getString("word")
      }
      log.warn(s"[loadSuccess] load user words from db count: ${words.size}\ttime elapsed: ${System.currentTimeMillis - preTime}")
      words
    } catch {
      case e: Exception =>
        log.error("[loadError] error: ", e)
        mutable.HashSet[String]()
    } finally {
      statement.close()
      conn.close()
    }
  }


  def main(args: Array[String]): Unit = {
       val set= MysqlService.getUserWords()
        print(  set )
  }
}
