package com.jd.spark.dataflow.kafka2orc.bean

import java.io.Serializable

class OutParam extends Serializable{
  //重载的构造器  
  def this(f:String){
    this() //必须得调用一次主构造器
    this.lastFile=f;
  }
  var isException:Boolean=false;
  var exception:String=null;
  var lastFile:String=null;
  var size:Long=0;
}