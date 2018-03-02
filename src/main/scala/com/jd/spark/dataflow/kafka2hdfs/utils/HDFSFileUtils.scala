package com.jd.spark.dataflow.kafka2hdfs.utils

import com.jd.spark.dataflow.kafka2hdfs.bean.OutParam;
import com.jd.spark.dataflow.kafka2hdfs.bean.InParam;
import com.jd.spark.dataflow.common.MyConstants;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.commons.lang3.math.NumberUtils
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.commons.lang3.StringUtils
import java.io.Serializable
import com.jd.spark.dataflow.utils.MyJsonCompressUtils
import java.util.concurrent.TimeUnit
import com.jd.spark.dataflow.utils.MyDateUtils
import org.apache.hadoop.fs.RemoteIterator
import org.apache.hadoop.fs.LocatedFileStatus


class HDFSFileUtils extends Serializable{
  @transient
  lazy val logger = Logger.getLogger(this.getClass.getName());

  //写成单利模式？？？
  def getFS():FileSystem={
    val config = new Configuration();
    config.setBoolean("dfs.support.append", MyConstants.DFS_SUPPORT_APPEND);
    config.setInt("dfs.client.socket-timeout", 900000);
    config.setInt("dfs.socket.timeout", 900000);
    config.setInt("dfs.datanode.socket.write.timeout", 900000);
    config.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
    config.set( "dfs.client.block.write.replace-datanode-on-failure.enable" ,"true" );
    val fs = FileSystem.get(config);
    return fs;
  }

  def closeFs(fs:FileSystem):Unit={
    if(fs!=null){//关闭异常 考虑getFS写成单利模式
      //fs.close();
    }
  }

  //获取最近分区，切判断分区文件大小是否过大，若是重新新建一个
  def getPartitionLastFile(inparam:InParam): OutParam ={
    val outParam:OutParam=new OutParam();
    val path=new Path(inparam.path);
    val fs=getFS();
    try{
      logger.info("get hive partition last file,path="+inparam.path);
      //优化这里以后加缓存，先从缓存里获取文件？？？？？========
      var curFile=null:String;
      var lastFile=null:String;
      var curFileStatus:FileStatus=null;
      //1.判断目录是否存在，不存在创建目录与0文件
      if(!fs.exists(path)){
        fs.mkdirs(path);
      }
      //2.目录存在，获取文件名最大的文件 查看文件大小 看是否需要创建新文件
      val fileIts=fs.listFiles(path, false);
      while(fileIts.hasNext()){
        curFileStatus=fileIts.next();
        curFile=curFileStatus.getPath().getName();
        if(NumberUtils.isDigits(curFile)){
          if(StringUtils.isEmpty(lastFile)){
            lastFile=curFile;
            outParam.size=curFileStatus.getLen();
            outParam.f=curFileStatus.getPath().toString();
          }else{
            //比较文件的大小
            if(NumberUtils.toLong(lastFile)<NumberUtils.toLong(curFile)){
              lastFile=curFile;
              outParam.size=curFileStatus.getLen();
              outParam.f=curFileStatus.getPath().toString();
            }
          }
        }
      }
      //判断
      if(StringUtils.isEmpty(lastFile)){//不存在文件，上面if判断里面创建的新目录
        outParam.f=inparam.path+MyConstants.DIR_SEPARATOR+"0";
        fs.createNewFile(new Path(outParam.f));
      }else if(outParam.size>MyConstants.HDFS_FILE_MAX_SIZE){//判断文件大小是否合格
        outParam.f=inparam.path+MyConstants.DIR_SEPARATOR+(NumberUtils.toLong(lastFile,999)+1);
        fs.createNewFile(new Path(outParam.f));
      }
    }catch {
      case t: Exception => {
        t.printStackTrace() // TODO: handle error
        logger.info(t.getMessage)
        outParam.isException=true;
        outParam.exception=t.getMessage;
      }
    }finally {
      closeFs(fs);
    }
    return outParam;
  }
  //向文件中写rdd
  def write(rdd:List[String],file:String):Boolean={
    logger.info("rdd write hdfs,file="+file);
    val fs=getFS();
    var rs:Boolean=true;
    var fin:FSDataOutputStream =null;
    try{
      fin = fs.append(new Path(file));
      rdd.foreach{str =>{
        fin.write((MyJsonCompressUtils.compress(str) + "\n").getBytes("UTF-8"));
      }
      }
      fin.close();
    }catch {
      case t: Exception => {
        t.printStackTrace() // TODO: handle error
        logger.error(t)
        rs=false;
      }
    }finally {
      if(fin!=null){
        try{
          fin.close();
        }catch {
          case t: Exception => {
            t.printStackTrace(); // TODO: handle error
            logger.error(t);
          }
        }
      }
      closeFs(fs);
    }
    return rs;
  }


  //把rdd中的手写到hdfs
  def processRDD(rdd:List[String],topic:String):Unit={
    var inParam:InParam=null;
    var partition:String=null;
    var outParam:OutParam =null;
    var bl:Boolean=true;
    var a=MyConstants.HDFS_RETRY_NUM;
    var b=MyConstants.HDFS_RETRY_NUM;
    inParam=new InParam();
    //测试时候按照分钟分区，线上按照小时分区
    partition=MyDateUtils.dateFormat();
    partition = "dt="+partition.replaceAll("/", "/ht=");
    inParam.path=MyConstants.HIVE_PATH_PRE+MyConstants.DIR_SEPARATOR+
      "type="+topic+MyConstants.DIR_SEPARATOR+
      partition;
    //判断是否成功
    while (a>=0) {
      outParam=getPartitionLastFile(inParam);
      if(outParam.isException){
        //失败
        a=a-1;
        TimeUnit.SECONDS.sleep(MyConstants.HDFS_RETRY_SLEEP_TIME);
      }else{
        //成功
        a= -1;
        while(b>=0){
          bl=write(rdd, outParam.f);
          if(bl){
            //成功
            b= -1;
          }else{
            //暂停重试
            b=b-1;
            TimeUnit.SECONDS.sleep(MyConstants.HDFS_RETRY_SLEEP_TIME);
          }
        }
      }
    }
  }

  //获取topic分区路径
  def getPartitionPath(hive_dir_pre:String,topic:String):String={
    var partition=MyDateUtils.dateFormat();
    partition = "dt="+partition.replaceAll("/", "/ht=");
    val path =hive_dir_pre+MyConstants.DIR_SEPARATOR+"type="+topic+MyConstants.DIR_SEPARATOR+partition;
    return path;
  }

  //获取指定topic，指定时间的路径
  def getPartitionPath(topic:String,ts:Long):String={
    var partition=MyDateUtils.dateFormat(ts);
    partition = "dt="+partition.replaceAll("/", "/ht=");
    val path ="type="+topic+MyConstants.DIR_SEPARATOR+
      partition;
    return path;
  }

  //移动文件
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