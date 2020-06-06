package project2_wordcounts.streamingWC.dao

import java.sql.Connection

import com.alibaba.druid.pool.DruidDataSource
import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.log4j.LogManager
import project2_wordcounts.streamingWC.utils.Conf

/**
 * Mysql连接池类
 *
 */
class MysqlPool extends Serializable {
  @transient lazy val log = LogManager.getLogger(this.getClass)

  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
  //private val dds=new DruidDataSource(  )
  private val conf = Conf.mysqlConfig
  try {
//    dds.setUsername(conf.get("username").getOrElse("root"))
//    dds.setPassword(conf.get("password").getOrElse("a"))
//    dds.setUrl(conf.get("url").getOrElse("jdbc:mysql://localhost:3306/word_freq?useUnicode=true&amp;characterEncoding=UTF-8"))
//    dds.setDriverClassName("com.mysql.jdbc.Driver")
//    dds.setInitialSize(   3 )
//    dds.setMaxActive(200)


    cpds.setJdbcUrl(conf.get("url").getOrElse("jdbc:mysql://localhost:3306/word_freq?useUnicode=true&amp;characterEncoding=UTF-8"));
    cpds.setDriverClass("com.mysql.jdbc.Driver");
    cpds.setUser(conf.get("username").getOrElse("root"));
    cpds.setPassword(conf.get("password").getOrElse("a"))
    cpds.setInitialPoolSize(3)
    cpds.setMaxPoolSize(Conf.maxPoolSize)
    cpds.setMinPoolSize(Conf.minPoolSize)
    cpds.setAcquireIncrement(5)
    cpds.setMaxStatements(180)
    /* 最大空闲时间,25000秒内未使用则连接被丢弃。若为0则永不丢弃。Default: 0 */
    cpds.setMaxIdleTime(25000)
    // 检测连接配置
    cpds.setPreferredTestQuery("select id from user_words limit 1")
    cpds.setIdleConnectionTestPeriod(18000)
  } catch {
    case e: Exception =>
      log.error("[MysqlPoolError]", e)
  }
  def getConnection: Connection = {
    try {
      return cpds.getConnection();
    } catch {
      case e: Exception =>
        log.error("[MysqlPoolGetConnectionError]", e)
        null
    }
  }
}
//单例模型
object MysqlManager {
  var mysqlManager: MysqlPool = _
  def getMysqlManager: MysqlPool = {
    synchronized {
      if (mysqlManager == null) {
        mysqlManager = new MysqlPool
      }
    }
    mysqlManager
  }
}
