package cn.com.gis.etl.shanghai

import java.text.SimpleDateFormat

import com.utils.ConfigUtils
import org.apache.spark.{SparkContext, SparkConf}

import scala.math._
import scala.collection.mutable.ArrayBuffer
import cn.com.gis.etl.shanghai.function.shfinger1LonLat
import com.utils.RedisUtils

/**
 * Created by wangxy on 15-11-27.
 */
object tongjilonlat {

  val propFile = "/config/shanghai.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val finger_line_max_num = prop.getOrElse("FINGER_LINE_MAX_NUM", "12").toInt
  val grip_size = prop.getOrElse("GRID_SIZE", "25").toInt
  val rssi_uplimit = prop.getOrElse("RSSI_UPLIMIT", "-40").toInt
  val rssi_downlimit = prop.getOrElse("RSSI_DOWNLIMIT", "-100").toInt

  val combineinfo_num = prop.getOrElse("COMBINEINFO_NUM", "2").toInt
  val combineinfo_timelimit = prop.getOrElse("COMBINEINFO_TIMELIMIT", "20000").toInt

  // 指纹库名称
  val bsLib_name = prop.getOrElse("bsLib_name", "shbaselib1")
  val neiLib_name = prop.getOrElse("neiLib_name", "shneilib1")
  val fingerlib_name = prop.getOrElse("fingerlib_name", "shfingerlib1")
  val tsmrsrplib_name = prop.getOrElse("rsrpLib_name", "tsmrsrplib1")


  def main(args: Array[String]): Unit = {

    val fingerinfo = RedisUtils.getResultMap(fingerlib_name)

    var num1 = 0
    var num2 = 0
    var num3 = 0
    var num4 = 0
    var num5 = 0
    var num6 = 0
    var num7 = 0
    var num8 = 0
    var num9 = 0
    var num10 = 0
    var num11 = 0
    var num12 = 0
    var num = 0
    fingerinfo.foreach{x=>
      num += 1
      val strArr = x._2.split("\\$", -1)
      if(strArr.length == 1){
        num1 += 1
      }
      if(strArr.length == 2){
        num2 += 1
      }
      if(strArr.length == 3){
        num3 += 1
      }
      if(strArr.length == 4){
        num4 += 1
      }
      if(strArr.length == 5){
        num5 += 1
      }
      if(strArr.length == 6){
        num6 += 1
      }
      if(strArr.length == 7){
        num7 += 1
      }
      if(strArr.length == 8){
        num8 += 1
      }
      if(strArr.length == 9){
        num9 += 1
      }
      if(strArr.length == 10){
        num10 += 1
      }
      if(strArr.length == 11){
        num11 += 1
      }
      if(strArr.length == 12){
//        val info = x._2.split("\\$", -1).map{_.split(",", -1)}
//        val a = info.distinct
//        println(s"a.length = ${a.length}")
//        println(s"info = ${x._2}")
        num12 += 1
      }
    }
    println(s"num1 = $num1")
    println(s"num2 = $num2")
    println(s"num3 = $num3")
    println(s"num4 = $num4")
    println(s"num5 = $num5")
    println(s"num6 = $num6")
    println(s"num7 = $num7")
    println(s"num8 = $num8")
    println(s"num9 = $num9")
    println(s"num10 = $num10")
    println(s"num11 = $num11")
    println(s"num12 = $num12")
    println(s"num = $num")

  }

}
