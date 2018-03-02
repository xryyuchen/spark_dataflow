package com.jd.spark.dataflow.kafka2orc.bean

import java.io.Serializable

class InParam extends Serializable{
  //重载的构造器  
  def this(path: String){  
    this() //必须得调用一次主构造器  
    this.path=path;  
  }
  var path:String=null;
}