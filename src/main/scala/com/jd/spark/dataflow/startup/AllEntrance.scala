package com.jd.spark.dataflow.startup

import org.apache.commons.cli.Options
import org.apache.commons.cli.Option
import org.apache.commons.cli.BasicParser
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.HelpFormatter
import org.apache.log4j.Logger
import com.jd.spark.dataflow.kafka2hdfs.consumers.SparkConsumerKafka
import com.jd.spark.dataflow.common.MyConstants

//命令行解析器
object AllEntrance {
  @transient
  lazy val logger = Logger.getLogger(AllEntrance.getClass.getName);

  def main(args: Array[String]): Unit = {

    val opts = new Options();
    var opt = new Option(MyConstants.HELP, MyConstants.HELP, false, "打印命令");
    opt.setRequired(false);
    opts.addOption(opt);
    opt = new Option(MyConstants.SERVICE, MyConstants.SERVICE, true, "启动服务的名字");
    opt.setRequired(true);
    opts.addOption(opt);
    opt = new Option(MyConstants.CONSUMER_TOPIC,MyConstants.CONSUMER_TOPIC, true, "topic名字");
    opt.setRequired(true);
    opts.addOption(opt);
    opt = new Option(MyConstants.KAFKA_BROKER_ID, MyConstants.KAFKA_BROKER_ID, true, "kafka集群名字1or2");
    opt.setRequired(true);
    opts.addOption(opt);
    opt = new Option(MyConstants.RUN_MODEL, MyConstants.RUN_MODEL, true, "测试或者在在线运行，test or online");
    opt.setRequired(false);
    opts.addOption(opt);
    opt = new Option(MyConstants.SPARK_MAX_RUN_JOBS, MyConstants.SPARK_MAX_RUN_JOBS, true, "spark.streaming.concurrentJobs");
    opt.setRequired(false);
    opts.addOption(opt);
    opt = new Option(MyConstants.SPARK_PARALLELISM, MyConstants.SPARK_PARALLELISM, true, "spark.default.parallelis");
    opt.setRequired(false);
    opts.addOption(opt);
    opt = new Option(MyConstants.SPARK_PRE_PARTITION_MAX, MyConstants.SPARK_PRE_PARTITION_MAX, true, "spark.streaming.kafka.maxRatePerPartition");
    opt.setRequired(false);
    opts.addOption(opt);
    opt = new Option(MyConstants.SPARK_RECEIVE_INTERVAL, MyConstants.SPARK_RECEIVE_INTERVAL, true, "spark 处理间隔");
    opt.setRequired(false);
    opts.addOption(opt);
    opt = new Option(MyConstants.SPARK_COALESCE, MyConstants.SPARK_COALESCE, true, "spark coalesce num");
    opt.setRequired(false);
    opts.addOption(opt);

    opt = new Option(MyConstants.SPARK_DRA_MIN, MyConstants.SPARK_DRA_MIN, true, "spark.streaming.dynamicAllocation.minExecutors");
    opt.setRequired(false);
    opts.addOption(opt);
    opt = new Option(MyConstants.SPARK_DRA_MAX, MyConstants.SPARK_DRA_MAX, true, "spark.streaming.dynamicAllocation.maxExecutors");
    opt.setRequired(false);
    opts.addOption(opt);
    opt = new Option(MyConstants.CONSUMER_GROUP_POS, MyConstants.CONSUMER_GROUP_POS, true, "consumer group postfix");
    opt.setRequired(false);
    opts.addOption(opt);
    opt = new Option(MyConstants.SPARK_LOG_LEVEL, MyConstants.SPARK_LOG_LEVEL, true, "spark log level");
    opt.setRequired(false);
    opts.addOption(opt);
    opt = new Option(MyConstants.SPARK_SCHEDULER_MODE, MyConstants.SPARK_SCHEDULER_MODE, true, "spark.scheduler.mode");
    opt.setRequired(false);
    opts.addOption(opt);




    val parser = new BasicParser();
    var cl:CommandLine=null;
    try{
      //          var argsTest = Array("-service", "kafka2hdfs", "-consumer_topic", "serverinfo","-kafka_broker_id","1")
      //          cl = parser.parse(opts, argsTest);
      cl = parser.parse(opts, args);
      if (cl.getOptions().length > 0) {
        if (cl.hasOption(MyConstants.HELP)) {
          val hf = new HelpFormatter();
          hf.printHelp("May Options", opts);
        }else {
          // 打印opts的名称和值
          System.out.println("--------------------------------------");
          logger.info("===========================");
          var opts = cl.getOptions();
          if (opts != null) {
            for (opt1 <-opts) {
              var name = opt1.getOpt();
              var value = cl.getOptionValue(name);
              System.out.println(name + "=>" + value);
              logger.info(name + "=>" + value);
            }
          }
          if("kafka2hdfs".equals(cl.getOptionValue(MyConstants.SERVICE))){
            //验证topic是否为空
            if(cl.getOptionValue(MyConstants.CONSUMER_TOPIC).isEmpty()){
              logger.info("input param error")
              System.exit(-1);
            }
            //运行程序
            new SparkConsumerKafka(cl).run();
          }
        }
      }else {
        logger.error("ERROR_NOARGS")
        System.err.println("ERROR_NOARGS");
      }
    }catch {
      case t: Throwable => {
        t.printStackTrace() ;// TODO: handle error
        logger.error(t);
      }
    }
  }
}