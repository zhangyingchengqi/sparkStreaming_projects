create database log_analysis;

--需求1: 筛选出所有的 error,利用spark sql 转存数据库 
--   id       level       method      content
use log_analysis;

CREATE TABLE `important_logs` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `log_level` varchar(255) NOT NULL,
  `method` varchar(255) NOT NULL,
  `content` varchar(500) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8;


--需求二: 累计这个级别 日志出现的总次数,不需要表，只直接输出

--需求三: 在以 1秒为批处理时间间隔, 这个级别在过去的3个时间窗口内，每两个 slide intervals的次数，输出.
