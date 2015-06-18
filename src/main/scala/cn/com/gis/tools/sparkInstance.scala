package cn.com.gis.tools

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by wangxy on 15-6-11.
 */

object sparkInstance{
  private val conf = new SparkConf().setAppName("sparkgis Application")
  private val sc = new SparkContext(conf)
  def getInstance: SparkContext = sc
}
