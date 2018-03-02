package com.jd.spark.dataflow.utils

import java.text.SimpleDateFormat
import java.util.Date

import com.jd.spark.dataflow.common.MyConstants
import org.apache.commons.lang3.StringUtils

object MyDateUtils {

  def dateFormat(time:Long, pattern: String):String={

    var newTime = time
    var newPattern = pattern
    if (newTime < 0)
      newTime = (new Date()).getTime
    if (StringUtils.isEmpty(pattern) )
      newPattern = MyConstants.DATE_PATTERN_DEFAULT


    val sdf:SimpleDateFormat = new SimpleDateFormat(newPattern)
    val date:String = sdf.format(new Date((newTime)))
    return date
  }

  def dateFormat(pattern: String):String={

    dateFormat(-1, pattern)
  }

  def dateFormat(time:Long):String={
    dateFormat(time,"")
  }

  def dateFormat():String={
    dateFormat(-1,"")
  }

}
