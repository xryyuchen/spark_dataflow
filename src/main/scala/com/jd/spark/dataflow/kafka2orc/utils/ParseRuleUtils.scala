package com.jd.spark.dataflow.kafka2orc.utils

import java.util.regex.Pattern

import com.jd.spark.dataflow.kafka2orc.bean.ParseRules
import org.apache.commons.lang.StringUtils

object ParseRuleUtils {

  //name^string^$.user.name^_tmp
  //解析参数
  def parseRules(str: String) : ParseRules = {

    if (StringUtils.isEmpty(str)) {
      return null;
    }
    val parseRules = new ParseRules();
    val strArr = str.split("\\^");
    if (strArr.length != 4) {
      return null;
    }
    parseRules.setFieldName(replaceEnter(strArr(0)));
    parseRules.setTypeName(strArr(1));
    parseRules.setRule(strArr(2));
    parseRules.setDefaultValue(replaceEnter(strArr(3)));

    return parseRules;
  }

  def replaceEnter(str: String): String = {
    var dest = ""
    if (str != null) {
      val p = Pattern.compile("\r|\n")
      val m = p.matcher(str)
      dest = m.replaceAll("")
    }
    dest
  }
}
