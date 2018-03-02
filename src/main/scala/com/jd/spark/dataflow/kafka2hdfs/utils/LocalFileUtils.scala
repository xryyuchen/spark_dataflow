package com.jd.spark.dataflow.kafka2hdfs.utils

import java.io._
import java.util
import java.util.Map

import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger

import scala.io.{BufferedSource, Source}

@SerialVersionUID(1L)
object LocalFileUtils extends Serializable {

  private val logger = Logger.getLogger(LocalFileUtils.getClass)

  def createLocalFileFromeJar(source: String, des: String, fillFile: util.Map[String, String]): Boolean = {
    var file: File = null;
    var pw: PrintWriter = null;
    var inStreams: BufferedSource = null;
    var line: String = null;
    var rs = false;
    try {

      file = new File(des)
      file.deleteOnExit()
      pw = new PrintWriter(file)
      inStreams = Source.fromFile(source)("UTF-8")

      for(tmpLine <- inStreams.getLines){
        if (StringUtils.isNotEmpty(tmpLine)){
          import scala.collection.JavaConversions._
          for (entry <- fillFile.entrySet) {
            if (tmpLine.contains(entry.getKey)){
              line = tmpLine.replace(entry.getKey, entry.getValue);
            }
          }
          logger.info("=========>" + line);
          pw.write(line);
          pw.write("\n");
        }
      }

      rs = true
    } catch {
      case e: Exception =>
        e.printStackTrace()
        System.exit(-1)
    } finally {
      if (pw != null) try
        pw.close()
      catch {
        case e: IOException =>
          e.printStackTrace()
          logger.error(e)
      }
      if (inStreams != null) try
        inStreams.close()
      catch {
        case e: IOException =>
          e.printStackTrace()
          logger.error(e)
      }
    }
    rs
  }
}
