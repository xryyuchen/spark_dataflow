package com.jd.spark.dataflow.kafka2orc.bean

import java.io.Serializable

import scala.beans.BeanProperty

class ParseRules extends Serializable {

  @BeanProperty var fieldName: String = null;
  @BeanProperty var typeName: String = null;
  @BeanProperty var rule: String = null;
  @BeanProperty var defaultValue: String = null;

}
