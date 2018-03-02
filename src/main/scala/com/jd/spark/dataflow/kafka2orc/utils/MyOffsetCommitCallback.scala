package com.jd.spark.dataflow.kafka2orc.utils

import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.Logger

import scala.collection.JavaConversions._


class MyOffsetCommitCallback extends OffsetCommitCallback with  Serializable{
  @transient
  lazy val logger = Logger.getLogger(this.getClass().getName());
  def onComplete(m: java.util.Map[TopicPartition, OffsetAndMetadata], e: Exception) {
    if (null != e) {
      // error
       logger.info("autor_commit_error=========>"+e)
       m.toMap.foreach(f=>logger.error("autor_commit_error=========>topic:"+f._1.topic()+",partition:"+f._1.partition()+",metadata:"+f._2.metadata()+",offset:"+f._2.offset()))
    }else {
      // success
       m.toMap.foreach(f=>logger.info("autor_commit_success=========>topic:"+f._1.topic()+",partition:"+f._1.partition()+",metadata:"+f._2.metadata()+",offset:"+f._2.offset()))
    }
  }
}