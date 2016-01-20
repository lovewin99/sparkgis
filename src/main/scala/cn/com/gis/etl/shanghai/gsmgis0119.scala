package cn.com.gis.etl.shanghai

/**
 * Created by wangxy on 16-1-19.
 */

import java.text.SimpleDateFormat

import org.apache.spark.{SparkContext, SparkConf}
import scala.math._
import scala.collection.mutable.ArrayBuffer
import cn.com.gis.etl.shanghai.function.fingergis0119
import com.utils.{ConfigUtils, RedisUtils}

object gsmgis0119 {

  val propFile = "/config/shanghai.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val grip_size = prop.getOrElse("GRID_SIZE", "25").toInt

  // 确定有的
  val mr_length = 42
  val mr_time_index = 0
  val mr_lac_index = 3
  val mr_ci_index = 4
  val mr_mrxlev_index = 9
  val mr_ta_index = 10

  val mr_nei_maxnum = 6

  //临时信息
  val Finger_length = 11
  val Sampling_index = 0
  val time_index = 1
  val lon_index = 2
  val lat_index = 3
  val lac_index = 4
  val ci_index = 5
  val bcch_index = 6
  val bsic_index = 7
  val ta_index = 8
  val relevsub_index = 9
  val ismcell_index = 10

  // 最大临区个数
  val nei_maxnum = 12

  //经纬度转墨卡托
  def lonLat2Mercator(lon : Double, lat : Double) : (Double, Double) = {
    val x = lon * 20037508.342789 / 180
    var y = log(tan((90+lat)*Pi/360)) / (Pi / 180)
    y = y * 20037508.34789 / 180
    (x, y)
  }

  def outOfChina(lon : Double, lat : Double) : Boolean ={
    lon < 72.004 || lon > 137.8347 || lat < 0.8293 || lat > 55.8271
  }


  //((采样点,时间,栅格),(标识,ta,ismain,rxlevsub))
  def tmpInmapProcess(in: String): ((String, String, String), Array[String]) = {
    val strArr = in.split(",", -1)
    if(Finger_length == strArr.length){
      val lon = strArr(lon_index).toDouble
      val lat = strArr(lat_index).toDouble
      if(!outOfChina(lon, lat)){
        val sampling = strArr(Sampling_index)
        val time = strArr(time_index)
        val lonlat = lon + "," + lat
        val flag = strArr(bcch_index) + "|" + strArr(bsic_index)
        val ta = strArr(ta_index)
        val ismain = strArr(ismcell_index)
        val rxlev = strArr(relevsub_index)
        val value1 = Array[String](flag, ta, ismain, rxlev)
        //        println("in="+in)
        ((sampling, time, lonlat), value1)
      }else {
        (("-1", "-1", "-1"), Array[String]())
      }
    }else {
      (("-1", "-1", "-1"), Array[String]())
    }
  }

  // (用户, (公共信息, 定位信息))
  // 公共信息: 时间,栅格,采样点  定位信息: 多个纹线   纹线: 标识,ta,ismain,rxlevsub
  def tmpreduceProcess(key: (String, String, String), Iter: Iterable[Array[String]]):
  (String, (Array[String], ArrayBuffer[ArrayBuffer[String]])) = {
    val time = key._2.replaceAll("[\\-. ]", "")
    val lonlat = key._3

    var lineArr2 = ArrayBuffer[ArrayBuffer[String]]()
    Iter.toList.groupBy(_(0)).foreach(x => {
      var lineArr1 = ArrayBuffer[String]()
      val sum = x._2.size
      val mRsrp = x._2./:(0) { (x, y) => x + y(3).toInt } / sum
      lineArr1 ++= x._2.sortBy(_(2)).reverse.head.slice(0, 3)
      lineArr1 += mRsrp.toString
      lineArr2 += lineArr1
    })
    //    println("linearr2="+lineArr2.map{_.mkString(",")}.mkString("$"))
    ("1", (Array[String](time,lonlat,key._1), lineArr2))
  }

  def main(args: Array[String]): Unit = {

    if(args.length != 2){
      System.out.print("error input: <input-path> <output-path>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("gsmgis0119 Application")
    val sc = new SparkContext(conf)

    val finfo = RedisUtils.getResultMap("yuanqugsm1")
    val finfo1 = finfo.map{x =>
      val arr = x._2.split("\\$", -1).map{_.split(",", -1)}
      (x._1, arr)
    }.toArray

    val info = sc.broadcast(finfo1)

    val textRDD = sc.textFile(args(0))
    val result = textRDD.mapPartitions{Iter => Iter.map{tmpInmapProcess}}.groupByKey().map{x => tmpreduceProcess(x._1, x._2)}.groupByKey().
      mapPartitions{Iter =>
      // (用户, (公共信息, 定位信息))
      // 公共信息: 时间,栅格,采样点  定位信息: 多个纹线   纹线: 标识,ta,ismain,rxlevsub
      Iter.map{
        case (imsi, uInfo) =>{
          val sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS")
          val locationInfo = uInfo.map{
            case (Array(time, lonlat, sampling), userFinger) =>{
              val (x, y) = fingergis0119.location(userFinger, info.value)
              val timestamp = sdf.parse(time).getTime
              (timestamp, (x,y), Array(lonlat, sampling, time))
            }
          }.filter(_._2  != ("-1", "-1")).toArray
          fingergis0119.twiceCompare(locationInfo).map{
            case (timestamp, (x,y), Array(lonlat, sampling, time)) => {
              val nowlonlat = fingergis0119.Mercator2lonlat(x.toInt*grip_size, y.toInt*grip_size)
              val srclonlat = lonlat.split(",")
              val tmpd = fingergis0119.calc_distance(nowlonlat._1, nowlonlat._2, srclonlat(0).toDouble, srclonlat(1).toDouble)
              //println(s"tmpd=${tmpd.toString}, $nowlonlat, ${srclonlat.mkString(",")}, $x, $y")
              Array[String](time, imsi,lonlat, nowlonlat._1.toString, nowlonlat._2.toString, x, y, tmpd.toString).mkString(",")
            }
          }.mkString("\n")
        }
      }
    }
    result.saveAsTextFile(args(1))
  }
}
