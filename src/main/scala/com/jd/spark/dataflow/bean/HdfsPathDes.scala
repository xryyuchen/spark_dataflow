package com.jd.spark.dataflow.bean

class HdfsPathDes extends Serializable{
    
  var path:String=null;
  var lastFileName:String=null;
  var fileSize:Long=0;//单位b
}
