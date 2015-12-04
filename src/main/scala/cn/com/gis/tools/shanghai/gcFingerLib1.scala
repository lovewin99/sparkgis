package cn.com.gis.tools.shanghai

import cn.com.gis.utils.tRedisPutMap
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.io.Source
import scala.math._
import com.utils.{RedisUtils, ConfigUtils}

/**
 * Created by wangxy on 15-11-19.
 */
object gcFingerLib1 {
  val propFile = "/config/shanghai.properties"
  val prop = ConfigUtils.getConfig(propFile)
  // 指纹库名称
  val bsLib_name = prop.getOrElse("bsLib_name", "shbaselib1")
  val neiLib_name = prop.getOrElse("neiLib_name", "shneilib1")
  val rsrpLib_name = prop.getOrElse("rsrpLib_name", "tsmrsrplib1")

  val gc_length = 13

  val freq_index = 0
  val pci_index = 1
  val eNB_index = 4
  val lon_index = 6
  val lat_index = 7

  val ee = 0.00669342162296594323
  val aM = 6378245.0

  //*******************************************************/
  // 地球坐标转火星坐标
  def outOfChina(lon : Double, lat : Double) : Boolean ={
    lon < 72.004 || lon > 137.8347 || lat < 0.8293 || lat > 55.8271
  }

  def transformLat(x : Double, y : Double) : Double={
    var ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * sqrt(abs(x))
    ret += (20.0 * sin(6.0 * x * Pi) + 20.0 * sin(2.0 * x * Pi)) * 2.0 / 3.0
    ret += (160.0 * sin(y / 12.0 * Pi) + 320 * sin(y * Pi / 30.0)) * 2.0 / 3.0
    ret
  }

  def transformLon(x : Double, y : Double) : Double={
    var ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * sqrt(abs(x))
    ret += (20.0 * sin(6.0 * x * Pi) + 20.0 * sin(2.0 * x * Pi)) * 2.0 / 3.0
    ret += (20.0 * sin(x * Pi) + 40.0 * sin(x / 3.0 * Pi)) * 2.0 / 3.0
    ret += (150.0 * sin(x / 12.0 * Pi) + 300.0 * sin(x / 30.0 * Pi)) * 2.0 / 3.0
    ret
  }

  // 也是地球坐标转火星坐标 wgs gcj
  def wgsTOgcj(wgLon : Double, wgLat : Double) : (Double, Double)={
    if(outOfChina(wgLon, wgLat)){
      (wgLon, wgLat)
    }else{
      var dLat = transformLat(wgLon - 105.0, wgLat - 35.0)
      var dLon = transformLon(wgLon - 105.0, wgLat - 35.0)
      val radLat = wgLat / 180.0 * Pi
      var magic = sin(radLat)
      magic = 1 - ee * magic * magic
      val sqrtMagic = sqrt(magic)
      dLat = (dLat * 180.0) / ((aM * (1 - ee)) / (magic * sqrtMagic) * Pi)
      dLon = (dLon * 180.0) / (aM / sqrtMagic * cos(radLat) * Pi)
      val mgLat = wgLat + dLat
      val mgLon = wgLon + dLon
      (mgLon, mgLat)
    }
  }

  //*******************************************************/
//  def wgsTOgcj(wgLon : Double, wgLat : Double) : (Double, Double)={
//    (wgLon, wgLat)
//  }


  def main(args: Array[String]): Unit = {
//    if (args.length != 3) {
//      System.err.println("Usage: <gongcan-file> <luce-file> <saopin-file>")
//      System.exit(1)
//    }

    val path = "/home/wangxy/data/gcshanghaiLTEcell222.csv"

//    val (baseInfo, neiInfo) = gcProcess("/home/wangxy/data/test1.txt")

    val basemap = Map[String, String]()
    val neimap = Map[String, ArrayBuffer[String]]()
    var num = 0
    for (line <- Source.fromFile(path).getLines) {
      val strArr = line.split(",", -1)
      if(gc_length == strArr.length){
        val pci_freq = strArr(pci_index) + "|" + strArr(freq_index)
        val eNB = strArr(eNB_index)
        val lon = strArr(lon_index)
        val lat = strArr(lat_index)
        val (nlon, nlat) = wgsTOgcj(lon.toDouble, lat.toDouble)
        basemap.put(eNB, nlon + "," + nlat)

        var arrB = neimap.getOrElse(pci_freq,  ArrayBuffer[String]())
        arrB += Array[String](eNB, nlon.toString, nlat.toString).mkString("|")
        neimap.put(pci_freq, arrB)
      }else{
        println(s"length error length=${strArr.length} line=$line")
        num += 1
      }
    }
    println(s"num = $num")

    val fneimap = neimap.map{x => (x._1 -> x._2.mkString(","))}.toMap

    RedisUtils.delTable(bsLib_name)
    RedisUtils.delTable(neiLib_name)

    tRedisPutMap.putMap2Redis(bsLib_name, basemap.toMap)
    tRedisPutMap.putMap2Redis(neiLib_name, fneimap)

    val path1 = "/home/wangxy/data/transformRsrp.csv"
    RedisUtils.delTable(rsrpLib_name)
    val ftsmmap = Source.fromFile(path1).getLines.map{in =>
      val strArr = in.split(",", -1)
      (strArr(0) -> strArr(1))
    }.toMap
    tRedisPutMap.putMap2Redis(rsrpLib_name, ftsmmap)

  }
}
