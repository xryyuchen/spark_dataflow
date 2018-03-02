package com.jd.spark.dataflow.kafka2hdfs.consumers


import java.util.HashMap
import java.util.concurrent.TimeUnit
import com.jd.spark.dataflow.common.MyConstants
import com.jd.spark.dataflow.kafka2hdfs.utils.{HDFSFileUtils, LocalFileUtils, MyOffsetCommitCallback}
import com.jd.spark.dataflow.utils.{MyDateUtils, MyJsonCompressUtils}
import org.apache.commons.cli.CommandLine
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.math.NumberUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}


class SparkConsumerKafka(cl:CommandLine) extends Serializable{
  @transient
  lazy val logger = Logger.getLogger(this.getClass().getName());
  def  run(): Unit ={
    //参数解析
    var  taskName=cl.getOptionValue(MyConstants.TASK_NAME);
    if(StringUtils.isEmpty(taskName)){
      taskName=this.getClass.getSimpleName;
    }

    logger.info("=======init_task_name:"+taskName);
    val topic=cl.getOptionValue(MyConstants.CONSUMER_TOPIC);
    //是否每次生成一个新的group
    var consumer_group_pos=cl.getOptionValue(MyConstants.CONSUMER_GROUP_POS);
    if("ts".equals(consumer_group_pos)){
      consumer_group_pos=MyDateUtils.dateFormat(MyConstants.DATE_PATTERN_DEFAULT2);
    }else{
      consumer_group_pos=MyConstants.CONSUMER_GROUP_POS_VALUE;
    }

    logger.info("=======topic:"+topic);
    var consumerGroup=cl.getOptionValue(MyConstants.CONSUMER_GROUP);
    var run_model=cl.getOptionValue(MyConstants.RUN_MODEL);
    var service=cl.getOptionValue(MyConstants.SERVICE);
    //这里添加变了  是否用原始的group 还是每次重新生成一个group ,有些消息比较大 重启启动报异常OOM
    if(StringUtils.isEmpty(consumerGroup)){
      if(StringUtils.isNotEmpty(run_model)){
        consumerGroup=topic+"-"+service+"-spark-dataflow-group-"+run_model;
      }else{
        consumerGroup=topic+"-"+service+"-spark-dataflow-group-"+MyConstants.KAFKA_GROUP_TEST;
      }
  }
    consumerGroup=consumerGroup+"-"+consumer_group_pos;
    logger.info("=======consumerGroup:"+consumerGroup);
    var hive_dir_pre=MyConstants.HIVE_PATH_PRE;
    var hive_dir_pre_tmp=MyConstants.HIVE_PATH_PRE_TMP;
    if(StringUtils.isEmpty(run_model)||run_model.contains("test")){
      hive_dir_pre=MyConstants.HIVE_PATH_PRE_TEST;
      hive_dir_pre_tmp=MyConstants.HIVE_PATH_PRE_TEST_TMP;
    }
    logger.info("=======hive_dir_pre:"+hive_dir_pre);
    var kafkaId=cl.getOptionValue(MyConstants.KAFKA_BROKER_ID);
    if("1".equals(kafkaId)){
      kafkaId=MyConstants.BROKER_LIST_1
    }else if("2".equals(kafkaId)){
      kafkaId=MyConstants.BROKER_LIST_2
    }else{
      logger.error("kafka.broker.id is error,kafka.broker.id="+kafkaId)
      kafkaId=MyConstants.BROKER_LIST_1
    }
    logger.info("=======kafkaId:"+kafkaId);
    var spark_max_run_jobs = cl.getOptionValue(MyConstants.SPARK_MAX_RUN_JOBS);
    var spark_max_run_jobs_num=MyConstants.SPARK_MAX_RUN_JOBS_NUM;
    if (StringUtils.isEmpty(spark_max_run_jobs) || !NumberUtils.isDigits(spark_max_run_jobs)) {
      spark_max_run_jobs_num = MyConstants.SPARK_MAX_RUN_JOBS_NUM;
    }else{
      spark_max_run_jobs_num =NumberUtils.toInt(spark_max_run_jobs, MyConstants.SPARK_MAX_RUN_JOBS_NUM) ;
    }
    logger.info("=======spark_max_run_jobs_num:"+spark_max_run_jobs_num);
    var  spark_parallelism = cl.getOptionValue(MyConstants.SPARK_PARALLELISM);
    var spark_parallelism_num=MyConstants.SPARK_PARALLELISM_NUM;
    if (StringUtils.isEmpty(spark_parallelism) || !NumberUtils.isDigits(spark_parallelism)) {
      spark_parallelism_num = MyConstants.SPARK_PARALLELISM_NUM;
    }else{
      spark_parallelism_num=NumberUtils.toInt(spark_parallelism, MyConstants.SPARK_PARALLELISM_NUM) ;
    }
    logger.info("=======spark_parallelism_num:"+spark_parallelism_num);
    var  spark_pre_partition_max = cl.getOptionValue(MyConstants.SPARK_PRE_PARTITION_MAX);
    var spark_pre_partition_max_num=MyConstants.SPARK_PRE_PARTITION_MAX_NUM
    if( StringUtils.isEmpty(spark_pre_partition_max) || !NumberUtils.isDigits(spark_pre_partition_max) ){
      spark_pre_partition_max_num = MyConstants.SPARK_PRE_PARTITION_MAX_NUM;
    }else{
      spark_pre_partition_max_num=NumberUtils.toInt(spark_pre_partition_max, MyConstants.SPARK_PRE_PARTITION_MAX_NUM) ;
    }
    logger.info("=======spark_pre_partition_max_num:"+spark_pre_partition_max_num);
    var max_poll_records=(spark_pre_partition_max_num).toString();//因为每个分区会启动一个consumer 所以不用乘以 spark_parallelism_num
    logger.info("=======max_poll_records:"+max_poll_records);
    var spark_receive_interval=cl.getOptionValue(MyConstants.SPARK_RECEIVE_INTERVAL);
    var spark_receive_interval_sec=MyConstants.SPARK_RECEIVE_INTERVAL_SEC;
    if( StringUtils.isEmpty(spark_receive_interval) || !NumberUtils.isDigits(spark_receive_interval) ){
      spark_receive_interval_sec = MyConstants.SPARK_RECEIVE_INTERVAL_SEC;
    }else{
      spark_receive_interval_sec=NumberUtils.toInt(spark_receive_interval, MyConstants.SPARK_RECEIVE_INTERVAL_SEC) ;
    }
    logger.info("=======spark_receive_interval_sec:"+spark_receive_interval_sec);
    var spark_coalesce =cl.getOptionValue(MyConstants.SPARK_COALESCE);
    var spark_coalesce_num=MyConstants.SPARK_COALESCE_NUM;
    if( StringUtils.isEmpty(spark_coalesce) || !NumberUtils.isDigits(spark_coalesce) ){
      spark_coalesce_num = MyConstants.SPARK_COALESCE_NUM;
    }else{
      spark_coalesce_num=NumberUtils.toInt(spark_coalesce, MyConstants.SPARK_COALESCE_NUM) ;
    }
    logger.info("=======spark_coalesce_num:"+spark_coalesce_num);
    var spark_dra_min =cl.getOptionValue(MyConstants.SPARK_DRA_MIN);
    var spark_dra_min_num=MyConstants.SPARK_DRA_MIN_NUM;
    if( StringUtils.isEmpty(spark_dra_min) || !NumberUtils.isDigits(spark_dra_min) ){
      spark_dra_min_num = MyConstants.SPARK_DRA_MIN_NUM;
    }else{
      spark_dra_min_num=NumberUtils.toInt(spark_dra_min, MyConstants.SPARK_DRA_MIN_NUM) ;
    }
    logger.info("=======spark_dra_min_num:"+spark_dra_min_num);
    var spark_dra_max =cl.getOptionValue(MyConstants.SPARK_DRA_MAX);
    var spark_dra_max_num=MyConstants.SPARK_DRA_MAX_NUM;
    if( StringUtils.isEmpty(spark_dra_max) || !NumberUtils.isDigits(spark_dra_max) ){
      spark_dra_max_num = MyConstants.SPARK_DRA_MAX_NUM;
    }else{
      spark_dra_max_num=NumberUtils.toInt(spark_dra_max, MyConstants.SPARK_DRA_MAX_NUM) ;
    }
    logger.info("=======spark_dra_max_num:"+spark_dra_max_num);
    var log_level=cl.getOptionValue(MyConstants.SPARK_LOG_LEVEL);
    if(StringUtils.isEmpty(log_level)){
      log_level=MyConstants.SPARK_LOG_LEVEL_VALUE;
    }
    logger.info("=======log_level:"+log_level);
    var spark_scheduler_mode=cl.getOptionValue(MyConstants.SPARK_SCHEDULER_MODE);
    if(StringUtils.isEmpty(spark_scheduler_mode)){
      spark_scheduler_mode=MyConstants.SPARK_SCHEDULER_MODE_VALUE;
    }
    logger.info("=======spark_scheduler_mode:"+spark_scheduler_mode);


    //设置spark streaming调度模式
    val scheduleFile="/tmp/fairscheduler.xml";
    val fillFileMap:HashMap[String,String]=new HashMap();
    fillFileMap.put("${spark_batch_scheduler_mode}",spark_scheduler_mode);
    if(spark_coalesce_num>0){
      fillFileMap.put("${spark_batch_minShare}", (spark_coalesce_num*spark_max_run_jobs_num)+"");
    }else{
      fillFileMap.put("${spark_batch_minShare}", (spark_parallelism_num*spark_max_run_jobs_num)+"");
    }
    val rs=LocalFileUtils.createLocalFileFromeJar("fairscheduler.xml", scheduleFile,fillFileMap);
    if(!rs){
      println("解析schedule xml文件失败");
      logger.error("解析schedule xml文件失败");
      System.exit(-1);
    }
    //spark 启动参数设置
    val builder = SparkSession.builder().appName(taskName)
    //spark 参数优化
    builder.config("spark.executor.heartbeatInterval","30s");//10s
    builder.config("spark.sql.broadcastTimeout",3000);//300
    builder.config("spark.network.timeout", 300000);//120s
    builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    builder.config("spark.kryoserializer.buffer.max", "2000m");//小于2048
    builder.config("spark.kryoserializer.buffer", "64k");
    builder.config("spark.driver.maxResultSize", "4g");
    builder.config("spark.default.parallelism", spark_parallelism_num);
    builder.config("spark.sql.shuffle.partitions", spark_parallelism_num);
    builder.config("spark.shuffle.blockTransferService", "nio");
    builder.config("spark.shuffle.registration.maxAttempts",10);//好像是2.3的配置
    builder.config("spark.shuffle.registration.timeout","2m");
    //spark  block
    builder.config("spark.streaming.blockInterval",1000);//默认200ms
    builder.config("spark.streaming.blockQueueSize",50);//默认10
    //spark 本地化
    builder.config("spark.locality.wait",5);//默认3
    //spark 并行度
    builder.config("spark.streaming.concurrentJobs", spark_max_run_jobs_num);
    builder.config("spark.scheduler.mode","FAIR");//  FIFO/FAIR
    builder.config("spark.scheduler.allocation.file", scheduleFile);//hdfs:///user/mart_mobile/wz/fairScheduler.xml
    //spark动态配置
    if(spark_dra_min_num==spark_dra_max_num){
      builder.config("spark.executor.instances",spark_dra_max_num);
    }else{
      builder.config("spark.shuffle.service.enabled",true);
      builder.config("spark.streaming.dynamicAllocation.enabled",true);
      builder.config("spark.streaming.dynamicAllocation.minExecutors",spark_dra_min_num);
      builder.config("spark.streaming.dynamicAllocation.maxExecutors",spark_dra_max_num);
      builder.config("spark.streaming.dynamicAllocation.scalingInterval","30s");
      builder.config("spark.streaming.dynamicAllocation.debug",true);
      builder.config("spark.streaming.dynamicAllocation.releaseRounds",10);
      builder.config("spark.streaming.dynamicAllocation.rememberBatchSize",1);//1
      builder.config("spark.streaming.dynamicAllocation.delay.rounds",Int.MaxValue);//10
      builder.config("spark.streaming.dynamicAllocation.reserveRate",0.2);//0.2
      builder.config("spark.streaming.dynamicAllocation.scalingUpRatio",0.7);//0.9
      builder.config("spark.streaming.dynamicAllocation.scalingDownRatio",0.1);//0.3
    }
    //spark yarn
    builder.config("spark.yarn.maxAppAttempts",10);//Driver自动重启次数,现在driver不能重启 重启异常，带解决,可以看线上效果 要不要和consumer_group_pos 结合配置
    builder.config("spark.yarn.am.attemptFailuresValidityInterval","1s");//计数器应该在每个小时都重置
    builder.config("spark.yarn.executor.failuresValidityInterval","1s");//http://www.mamicode.com/info-detail-2028029.html
    builder.config("spark.yarn.max.executor.failures",Int.MaxValue-1000);//这个可以配置 8 * num_executors，因为上面有1小时刷新策略
    builder.config("spark.task.maxFailures",5);//最大失败次数 默认4
    builder.config("spark.hadoop.fs.hdfs.impl.disable.cache",true);//禁用HDFS缓存
    //spark 反压
    builder.config("spark.streaming.backpressure.initialRate", 100);
    builder.config("spark.streaming.backpressure.enabled", true);
    builder.config("spark.streaming.backpressure.pid.minRate",100);//默认100
    //spark 推断执行
    builder.config("spark.speculation",true);//推断执行
    builder.config("spark.speculation.interval",100);//100ms
    builder.config("spark.speculation.quantile",0.5);//完成task的百分比时启动推测
    builder.config("spark.speculation.multiplier",1.5);//比其他的慢多少倍时启动推测
    //spark writeAheadLog
    builder.config("spark.streaming.receiver.writeAheadLog.enable", false);
    builder.config("spark.streaming.driver.writeAheadLog.allowBatching",false);//true
    builder.config("spark.streaming.driver.writeAheadLog.batchingTimeout",60000);//5000
    builder.config("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", true);
    builder.config("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", true);
    //spark 优雅关闭
    builder.config("spark.streaming.stopGracefullyOnShutdown", true);
    //spark 压缩
    builder.config("spark.broadcast.compress", true);
    builder.config("spark.rdd.compress",true);
    builder.config("spark.io.compression.codec","org.apache.spark.io.SnappyCompressionCodec");//org.apache.spark.io.LZFCompressionCodec org.apache.spark.io.SnappyCompressionCodec
    //spark kafka
    builder.config("spark.streaming.kafka.maxRatePerPartition", spark_pre_partition_max_num);
    builder.config("spark.streaming.kafka.consumer.cache.enabled",false);
    builder.config("spark.streaming.kafka.consumer.cache.maxCapacity",spark_max_run_jobs_num*spark_parallelism_num);//64  2.2中可用
    builder.config("spark.streaming.kafka.maxRetries", 3);
    builder.config("spark.streaming.kafka.consumer.poll.ms", 5000);//512


    val session=builder.getOrCreate();
    val sc = session.sparkContext;
    sc.setLocalProperty("spark.scheduler.pool","spark_batch");
    sc.setLogLevel(log_level);//设置日志级别
    val ssc = new StreamingContext(sc, Seconds(spark_receive_interval_sec));
    ssc.checkpoint(MyConstants.CHECKPOINT_DIR
      +MyConstants.DIR_SEPARATOR+topic
      +MyConstants.DIR_SEPARATOR+consumerGroup);

    //kafka参数设置
    val kafkaParam:Map[String,Object] = Map(
      "bootstrap.servers" -> kafkaId,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> consumerGroup,
      "auto.offset.reset" -> MyConstants.AUTO_OFFSET_RESET,
      "enable.auto.commit" -> (MyConstants.ENABLE_AUTO_COMMIT: java.lang.Boolean),//有saprk管理 所以设置为false
      "auto.commit.interval.ms"->"5000",//默认是5000
      "max.poll.records"-> max_poll_records,
      "max.poll.interval.ms"->"600000",
      "max.partition.fetch.bytes"-> MyConstants.MAX_FETCH_BYTES,//1G  max.poll.records 用这个控制每次获取数据大小
      "fetch.message.max.bytes"->MyConstants.MAX_FETCH_BYTES,
      "fetch.max.bytes"->MyConstants.MAX_FETCH_BYTES,
      "fetch.min.bytes"->"1",
      "request.timeout.ms"->"600000",//305s
      "session.timeout.ms"->"120000",//10s
      "fetch.max.wait.ms"->"600",
      "heartbeat.interval.ms"->"30000"//3000
    );
    val schema = new StructType()
      .add("key", StringType,true)
      .add("value",StringType,true);
    val fs =new HDFSFileUtils();
    val realTimeDStream = KafkaUtils.createDirectStream[String,String](ssc, LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam));
    //手动提交
    realTimeDStream.foreachRDD{ rdd =>
    {
      if(!rdd.isEmpty()){
        var a=MyConstants.HDFS_RETRY_NUM;
        //提交offset 保存在kafka中
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges;
        var rows:RDD[Row]=null;
        var ts:Long=0;
        var partition:String=null;
        var tmpPath:String=null;
        var realPath:String=null;
        while(a>0){
          try{
            ts=System.currentTimeMillis();
            partition=fs.getPartitionPath(topic,ts);
            tmpPath=hive_dir_pre_tmp+MyConstants.DIR_SEPARATOR+partition+MyConstants.DIR_SEPARATOR+ts;
            realPath=hive_dir_pre+MyConstants.DIR_SEPARATOR+partition;
            ts=System.currentTimeMillis();
            if(spark_coalesce_num>0){
              rows=rdd.map(f=>Row(f.key(),MyJsonCompressUtils.compress(f.value()))).coalesce(spark_coalesce_num,false);
            }else{
              rows=rdd.map(f=>Row(f.key(),MyJsonCompressUtils.compress(f.value())));
            }
            val df=session.createDataFrame(rows, schema);
            df.select("value").write.option("compression", "none").mode(SaveMode.Append).json(tmpPath);
            fs.renameMV(tmpPath, realPath, ts,true);
            a= -1;
          }catch {
            case t: Exception => {
              t.printStackTrace(); // TODO: handle error
              logger.error("rdd_process_error ======>失败次数:"+a+"<=======>"+t);
              a=a-1;
              TimeUnit.SECONDS.sleep(MyConstants.HDFS_RETRY_SLEEP_TIME);
            }
          }finally{
            realTimeDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges,new MyOffsetCommitCallback());
          }
        }
      }
    }
    };
    ssc.start();
    ssc.awaitTermination();
  }
}