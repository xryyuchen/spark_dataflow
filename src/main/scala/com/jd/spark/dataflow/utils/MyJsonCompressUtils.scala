package com.jd.spark.dataflow.utils

import org.apache.commons.lang.StringUtils

import util.control.Breaks._

object MyJsonCompressUtils extends Serializable {

  def compress(json: String): String = {

    if (StringUtils.isEmpty(json)) {
       return "";
    }
    var sb = new StringBuilder;
    var skip = true;
    // true 允许截取(表示未进入string双引号)
    var escaped = false; // 转义符
      for (i <- 0 until json.length()) {
        breakable {
          val c = json.charAt(i);
          if (!escaped && c == '\\') {
            escaped = true;
          } else {
            escaped = false;
          }
          if (skip) {
            if (c == ' ' || c == '\r' || c == '\n' || c == '\t') {
              break();
            }
          }
          sb.append(c);
          if (c == '"' && !escaped) {
            skip = !skip;
          }
        }
      }
    return sb.toString().replaceAll("\r\n", "\\\\r\\\\n");
  }
}
