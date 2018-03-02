package com.jd.spark.dataflow.common

import java.io.File
import java.util.concurrent.TimeUnit

object MyConstants {

  /**
    * 共用常量
    */
  val DIR_SEPARATOR = File.separator;

  /**
    * hdfs配置相关的常量
    */
  val DFS_SUPPORT_APPEND =true;
//  val HIVE_PATH_PRE = "hdfs://ns7/user/mart_mobile/fdm.db/fdm_cmo_appcore_business_logs";
  val HIVE_PATH_PRE = "/user/hive/warehouse/keywords.db";
  val HIVE_PATH_PRE_TMP = "hdfs://ns7/user/mart_mobile/fdm.db/fdm_cmo_appcore_business_logs_tmp";
  val HIVE_PATH_PRE_TEST = "hdfs://ns7/user/mart_mobile/tmp.db/fdm_cmo_appcore_business_logs";
  val HIVE_PATH_PRE_TEST_TMP = "hdfs://ns7/user/mart_mobile/tmp.db/fdm_cmo_appcore_business_logs_tmp";
  val CHECKPOINT_DIR="hdfs://ns7/user/mart_mobile/tmp.db/tmp_spark_checkpoint";
  val HDFS_FILE_MAX_SIZE=1024 * 1024   * 100;//100M
  val HDFS_RETRY_NUM=10;//hdfs操作最大重试次数
  val HDFS_RETRY_SLEEP_TIME=1;//重试过程中间隔

  /**
    * kafka配置相关的常量
    */
  val BROKER_LIST_1 = "172.20.147.121:9091,172.20.147.119:9091,172.20.147.106:9091,172.20.147.115:9091,172.28.149.149:9091,172.28.149.132:9091,172.28.149.142:9091,172.28.149.140:9091,172.28.149.138:9091,172.28.149.134:9091,172.28.149.144:9091,172.28.149.147:9091";
  val BROKER_LIST_2 = "172.20.147.116:9091,172.20.147.117:9091,172.28.149.139:9091,172.28.149.141:9091,172.28.149.143:9091";
  val AUTO_OFFSET_RESET = "latest";//earliest latest
  val ENABLE_AUTO_COMMIT = false;//如果为false 需要自己实现offset操作
  val KAFKA_GROUP_TEST="-spark-dataflow-group-test";
  val MAX_FETCH_BYTES=(1024*1024*1024*1).toString();//1G

  /**
    * sparkstreaming配置相关常量
    */
  val  SPARK_PRE_PARTITION_MAX_NUM=30000;
  val  SPARK_MAX_RUN_JOBS_NUM=3;
  val  SPARK_PARALLELISM_NUM=100;
  val  SPARK_RECEIVE_INTERVAL_SEC=5;
  val  SPARK_COALESCE_NUM=0;
  val  SPARK_DRA_MIN_NUM=0;
  val  SPARK_DRA_MAX_NUM=2;
  val  CONSUMER_GROUP_POS_VALUE="default";
  val  SPARK_LOG_LEVEL_VALUE="WARN";
  val  SPARK_SCHEDULER_MODE_VALUE="FAIR";


  /**
    * cli 命令参数
    *
    */
  val HELP="help";
  val SERVICE="service";
  val CONSUMER_TOPIC="consumer_topic";
  val KAFKA_BROKER_ID="kafka_broker_id";
  val TASK_NAME="task_name";
  val CONSUMER_GROUP="consumer_group";
  val RULES_FILE_PATH = "rules_file_path";
  val RUN_MODEL="run_model";
  val SPARK_MAX_RUN_JOBS="spark_max_run_jobs";
  val SPARK_PARALLELISM="spark_parallelism";
  val SPARK_PRE_PARTITION_MAX="spark_pre_partition_max";
  val SPARK_RECEIVE_INTERVAL="spark_receive_interval";
  val SPARK_COALESCE="spark_coalesce";
  val SPARK_DRA_MIN="spark_dra_min";
  val SPARK_DRA_MAX="spark_dra_max";
  val CONSUMER_GROUP_POS="consumer_group_pos";
  val SPARK_LOG_LEVEL="spark_log_level";
  val SPARK_SCHEDULER_MODE="spark_scheduler_mode";

  /**
    * 时间格式相关常量
    */

  val DATE_PATTERN_DEFAULT = "yyyyMMdd/HH";
  val DATE_PATTERN_DEFAULT2 = "yyyyMMddHHmmss";

  /**
    * 缓存配置相关常量
    *
    */

  val INITIALCAPACITY = 200  //缓存大小
  val CONCURRENCYLEVEL = 20  //并发数
  val EXPIREAFTERWRITE =  60  //cache存活时间
  val TIME_UNIT = TimeUnit.MINUTES    //时间单位

}
