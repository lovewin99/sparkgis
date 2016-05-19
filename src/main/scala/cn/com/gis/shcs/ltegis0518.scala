package cn.com.gis.shcs

import java.text.SimpleDateFormat

import com.utils.{ConfigUtils, RedisUtils}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.math._

/**
 * spark-submit --master spark://cloud138:7077 --executor-memory 8g --jars redisclient_2.10-2.12.jar,commons-pool-1.5.6.jar --driver-class-path redisclient_2.10-2.12.jar:commons-pool-1.5.6.jar --class cn.com.gis.shcs.ltegis0518 sparkgis_2.10-1.0.jar wxy/gsm0819 wxy/gsmout1
 * Created by wangxy on 16-5-18.
 */
object ltegis0518 {
  val propFile = "/config/shanghai.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val grip_size = prop.getOrElse("GRID_SIZE", "25").toInt



  val time_index = 13
  val imsi_index = 5
  val bcch_index = 16
  val bsic_index = 17
  val rsrpdlsup_index = 31
  val ta_index = 39
  val neinum_index = 42

  // 临区信息长度
  val neilength = 6
  // 临区数据相对位置
  val neibcch_index = 4
  val neibsic_index = 3
  val neirsrp_index = 6

  def etlmap(in: String): (String, (Array[String], ArrayBuffer[ArrayBuffer[String]])) = {
    val strArr = in.split(",", -1)
    if(strArr(rsrpdlsup_index) != "0"){
      val fingerArr = ArrayBuffer[ArrayBuffer[String]]()
      val mflag = strArr(bcch_index) + "|" + strArr(bsic_index)
      val mta = strArr(ta_index)
      val mrsrp = strArr(rsrpdlsup_index).toInt - 111
      val imsi = strArr(imsi_index)
      val time1 = strArr(time_index)
      fingerArr += ArrayBuffer[String](mflag, mta, "1", mrsrp.toString)
      for(i <- 0 to strArr(neinum_index).toInt-1){
        val index = neinum_index + neilength * i
        val neirsrp = strArr(index+neirsrp_index)
        if(neirsrp != "0"){
          val nflag = strArr(index+neibcch_index) + "|" + strArr(index+neibsic_index)
          val nrsrp = neirsrp.toInt - 111
          fingerArr += ArrayBuffer[String](nflag, mta, "0", nrsrp.toString)
        }
      }
      (imsi, (Array[String](time1), fingerArr))
    }else{
      ("", (Array[String](), ArrayBuffer[ArrayBuffer[String]]()))
    }
  }

  def main(args: Array[String]): Unit = {

    if(args.length != 2){
      System.out.print("error input: <input-path> <output-path>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("gsmgis0119 Application")
    val sc = new SparkContext(conf)

    val finfo = RedisUtils.getResultMap("shgsm0401")
    val finfo1 = finfo.map{x =>
      val arr = x._2.split("\\$", -1).map{_.split(",", -1)}
      (x._1, arr)
    }.toArray

    val info = sc.broadcast(finfo1)

    val textRDD = sc.textFile(args(0))
    val result = textRDD.map(etlmap).filter(_._1 != "").groupByKey().
      mapPartitions{Iter =>
      // (用户, (公共信息, 定位信息))
      // 公共信息: 时间,栅格,采样点  定位信息: 多个纹线   纹线: 标识,ta,ismain,rxlevsub
      Iter.map{
        case (imsi, uInfo) =>{
          val sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")
          val locationInfo = uInfo.map{
            case (Array(time), userFinger) =>{
              val (x, y) = fingergis0119.location(userFinger, info.value)
              val timeformat = sdf.format(time.toLong)
              (time.toLong, (x,y), Array(timeformat))
            }
          }.filter(_._2  != ("-1", "-1")).toArray
          fingergis0119.twiceCompare(locationInfo).map{
            case (timestamp, (x,y), Array(time)) => {
              val nowlonlat = fingergis0119.Mercator2lonlat(x.toInt*grip_size, y.toInt*grip_size)
              Array[String](time, nowlonlat._1.toString, nowlonlat._2.toString, x, y, imsi).mkString(",")
            }
          }.mkString("\n")
        }
      }
    }
    result.saveAsTextFile(args(1))
  }
}
