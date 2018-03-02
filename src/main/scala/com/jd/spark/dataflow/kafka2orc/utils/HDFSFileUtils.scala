package com.jd.spark.dataflow.kafka2orc.utils

import java.io.Serializable
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.jd.spark.dataflow.common.MyConstants
import com.jd.spark.dataflow.kafka2orc.bean.ParseRules
import com.jd.spark.dataflow.utils.MyDateUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.hive.ql.exec.vector._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.Logger
import org.apache.orc.{CompressionKind, OrcFile, TypeDescription}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer


class HDFSFileUtils extends Serializable {
  @transient
  lazy val logger = Logger.getLogger(this.getClass.getName());
  private var fs: FileSystem = null;

  @transient
  val config = new Configuration();

  private def getFS(): FileSystem = this.synchronized {
    if (fs == null) {
      config.setBoolean("dfs.support.append", MyConstants.DFS_SUPPORT_APPEND);
      config.setInt("dfs.client.socket-timeout", 600000);
      config.setInt("dfs.socket.timeout", 600000);
      config.setInt("dfs.datanode.socket.write.timeout", 600000);
      fs = FileSystem.get(config);
    }
    return fs;
  }

  def closeFs(fs: FileSystem): Unit = {
    if (fs != null) {
      //      fs.close();
    }
  }

  //向文件系统中写orc文件
  def write(rdd: RDD[ConsumerRecord[String, String]], file: String, beanArr: ArrayBuffer[ParseRules]): Boolean = {

    var rs: Boolean = true;
    val jsonPathUtils = new JsonPathUtils();

    try {

      rdd.foreachPartition { items => {

        @transient
        val conf = new Configuration();
        val schemaStr = new StringBuffer();
        schemaStr.append("struct<")
        for (i <- beanArr.indices) {

          schemaStr.append(beanArr(i).fieldName + ":" + beanArr(i).typeName + ",");
        }
        schemaStr.deleteCharAt(schemaStr.lastIndexOf(","));
        schemaStr.append(">");

        //orc格式
        //struct<containerIp:string,cpuUse:double,cpuset:string,hostIp:string,localCluster:string,localService:string,stamp:int,totalCoreNum:int,totalMem:int,type:int>
        val schema = TypeDescription.fromString(schemaStr.toString);


        val batch: VectorizedRowBatch = schema.createRowBatch();
        val writer = OrcFile.createWriter(new Path(file + UUID.randomUUID().toString),
          OrcFile.writerOptions(conf)
            .setSchema(schema)
            .blockSize(1024 * 1024 * 128)
            .compress(CompressionKind.SNAPPY));

        for (item <- items) {

          val row = batch.size;
          val ctx = jsonPathUtils.parse(item.value());

          //一个bean是一条数据的一列
          for (i <- beanArr.indices) {

            if ("double".equals(beanArr(i).typeName.toLowerCase)) {
              batch.cols(i).asInstanceOf[DoubleColumnVector].vector(row) = jsonPathUtils.parse2Double(ctx, beanArr(i).rule, beanArr(i).defaultValue.trim.toDouble);
            } else if ("int".equals(beanArr(i).typeName.toLowerCase)) {
              batch.cols(i).asInstanceOf[LongColumnVector].vector(row) = jsonPathUtils.parse2Long(ctx, beanArr(i).rule, beanArr(i).defaultValue.trim.toLong);
            } else if ("list".equals(beanArr(i).typeName.toLowerCase)) {
              batch.cols(i).asInstanceOf[BytesColumnVector].setVal(row, jsonPathUtils.parse2List(ctx, beanArr(i).rule, beanArr(i).defaultValue).getBytes, 0, jsonPathUtils.parse2String(ctx, beanArr(i).rule, beanArr(i).defaultValue).getBytes.length);
            } else {
              batch.cols(i).asInstanceOf[BytesColumnVector].setVal(row, jsonPathUtils.parse2String(ctx, beanArr(i).rule, beanArr(i).defaultValue).getBytes, 0, jsonPathUtils.parse2String(ctx, beanArr(i).rule, beanArr(i).defaultValue).getBytes.length);

            }
          }
          batch.size = batch.size + 1;
          if (batch.size == batch.getMaxSize) {
            writer.addRowBatch(batch);
            batch.reset();
          }
        }
        if (batch.size != 0) {
          writer.addRowBatch(batch);
          batch.reset();
        }
        writer.close();
      }
      }

    } catch {
      case t: Throwable => {
        t.printStackTrace() // TODO: handle error
        logger.info(t)
        rs = false;
      }
    } finally {

    }
    return rs;
  }


  //读取规则文本
  def readFileOnHDFS(strPath: String) : ArrayBuffer[ParseRules] = {

    var result : ArrayBuffer[ParseRules] = null;
    val fs = FileSystem.get(config);
    try {

      val hdfsInStream = fs.open(new Path(strPath));

      val size = hdfsInStream.available();
      val buffer = new Array[Byte](size);

      hdfsInStream.read(buffer);

      val rulesStr = new String(buffer, "UTF-8");

      result = parseStr(rulesStr);

      hdfsInStream.close();
      fs.close();
    } catch {
      case t: Exception => {
        t.printStackTrace(); // TODO: handle error
        logger.info(t.getMessage);
      }
    } finally {
      closeFs(fs);
    }

    return result;
  }


  def parseStr(str: String) : ArrayBuffer[ParseRules] = {

    val beanArr = new ArrayBuffer[ParseRules]()
    val rulesArr = str.split(",");

    for (i <- 0 until rulesArr.length){
      beanArr += ParseRuleUtils.parseRules(rulesArr(i));

    }

    return beanArr;
  }

  //获取分区
  def getPartitionPath(topic:String,ts:Long):String={
    var partition=MyDateUtils.dateFormat(ts);
    partition = "dt="+partition.replaceAll("/", "/ht=");
    val path ="type="+topic+MyConstants.DIR_SEPARATOR+
      partition;
    return path;
  }

  //将临时目录数据放入实际目录
  def renameMV(srcDir:String ,dstDir:String,ts:Long,isDelete:Boolean):Unit={
    var a=MyConstants.HDFS_RETRY_NUM;
    var  fs:FileSystem=null;
    var filename:String=null;
    var curFileStatus:FileStatus=null;
    var oldName:String=null;
    var newName:String=null;
    var srcPath:Path=null;
    var  dstpath:Path=null;
    var fileIts:RemoteIterator[LocatedFileStatus]=null;
    while(a>0){
      try{
        fs=getFS();
        srcPath=new Path(srcDir);
        if(!fs.exists(srcPath)){
          return ;
        }
        dstpath=new Path(dstDir);
        if(!fs.exists(dstpath)){
          fs.mkdirs(dstpath);
        }
        fileIts=fs.listFiles(srcPath, false);
        while(fileIts.hasNext()){
          curFileStatus=fileIts.next();
          if(curFileStatus.isFile()){
            oldName=curFileStatus.getPath().getName();
            newName=ts+"_"+oldName;
            fs.rename(new Path(srcDir+MyConstants.DIR_SEPARATOR+oldName), new Path(dstDir+MyConstants.DIR_SEPARATOR+newName));
          }
        }
        if(isDelete){
          fs.delete(srcPath,isDelete);
        }
        a= -1;
      }catch {
        case t: Exception => {
          t.printStackTrace(); // TODO: handle error
          logger.error("hdfs rename error,srcDir:"+srcDir
            +"dstDir:"+dstDir+
            "process_n:"+a
            +"=======>"+t);
          a=a-1;
          TimeUnit.SECONDS.sleep(MyConstants.HDFS_RETRY_SLEEP_TIME);
        }
      }finally {
        closeFs(fs);
      }
    }
  }
}