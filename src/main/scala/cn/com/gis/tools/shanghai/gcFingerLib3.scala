package cn.com.gis.tools.shanghai

import cn.com.gis.utils.tRedisPutMap
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.io.Source
import scala.math._
import com.utils.{RedisUtils, ConfigUtils}

/**
 * Created by wangxy on 15-12-7.
 */
object gcFingerLib3 {
  val propFile = "/config/shanghai.properties"
  val prop = ConfigUtils.getConfig(propFile)
  // 指纹库名称
  //  val bsLib_name = prop.getOrElse("bsLib_name", "shbaselib1")
  //  val neiLib_name = prop.getOrElse("neiLib_name", "shneilib1")
  //  val rsrpLib_name = prop.getOrElse("rsrpLib_name", "tsmrsrplib1")

  val bsLib_name = "shbaselib2"
  val neiLib_name = "shneilib2"
  val rsrpLib_name = "tsmrsrplib2"

  val gc_length = 13

  val freq_index = 0
  val pci_index = 1
  val eNB_index = 4
  val lon_index = 6
  val lat_index = 7

  val ee = 0.00669342162296594323
  val aM = 6378245.0

  // 站距 m
  val dis_limit = 3000.0

  def calc_distance(lon1 : Double, lat1 : Double, lon2 : Double, lat2 : Double) : Double={
    val Rc = 6378137.00  // 赤道半径
    val Rj = 6356725     // 极半径

    val radLo1 = lon1 * Pi / 180
    val radLa1 = lat1 * Pi / 180
    val Ec1 = Rj + (Rc - Rj) * (90.0 - lat1) / 90.0
    val Ed1 = Ec1 * cos(radLa1)

    val radLo2 = lon2 * Pi / 180
    val radLa2 = lat2 * Pi / 180

    val dx = (radLo2 - radLo1) * Ed1
    val dy = (radLa2 - radLa1) * Ec1
    val dDeta = sqrt(dx * dx + dy * dy)
    dDeta
  }

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

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: <in-file> <out-file>")
      System.exit(1)
    }

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val gcRDD = sc.textFile(args(0), 10)
    val rsrpRDD = sc.textFile(args(1), 10)
    gcRDD.cache()

    RedisUtils.delTable(bsLib_name)
    RedisUtils.delTable(neiLib_name)

    gcRDD.foreachPartition{Iter =>
      val basemap = Map[String, String]()
      Iter.foreach{l =>
        val strArr = l.split(",", -1)
        val eNB = strArr(eNB_index)
        val lon = strArr(lon_index)
        val lat = strArr(lat_index)
        val (nlon, nlat) = wgsTOgcj(lon.toDouble, lat.toDouble)
        val value = Array[String](nlon.toString, nlat.toString).mkString(",")
        basemap.put(eNB, value)
      }
      tRedisPutMap.putMap2Redis(bsLib_name, basemap.toMap)
    }

    gcRDD.cartesian(gcRDD).mapPartitions{Iter =>
      Iter.map{
        case (lin, rin) =>{
          var key = ""
          var value = (0.0, "")
          val lArr = lin.split(",", -1)
          val rArr = rin.split(",", -1)
          if(lArr(eNB_index) != rArr(eNB_index)){
            val d = calc_distance(lArr(lon_index).toDouble, lArr(lat_index).toDouble, rArr(lon_index).toDouble, rArr(lat_index).toDouble)
            val pci_freq = rArr(pci_index) + "|" + rArr(freq_index)
            if(d < dis_limit){
              key = Array[String](lArr(eNB_index), pci_freq).mkString(",")
              value = (d, rArr(eNB_index))
            }
          }
          (key, value)
        }
      }
    }.filter(_._1 != "").groupByKey(10).foreachPartition{Iter =>
      val neimap = Map[String, String]()
      Iter.foreach{
        case (key, info) => {
          val neNB = info.toArray.sortBy(_._1).head._2
          neimap.put(key, neNB)
        }
      }
      tRedisPutMap.putMap2Redis(neiLib_name, neimap.toMap)
    }

    RedisUtils.delTable(rsrpLib_name)
    val ftsmmap = rsrpRDD.map{in =>
      val strArr = in.split(",", -1)
      (strArr(0) -> strArr(1))
    }.collect().toMap
    tRedisPutMap.putMap2Redis(rsrpLib_name, ftsmmap)
  }
}
