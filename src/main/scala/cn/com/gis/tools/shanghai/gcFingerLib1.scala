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
        basemap.put(eNB, lon + "," + lat)

        var arrB = neimap.getOrElse(pci_freq,  ArrayBuffer[String]())
        arrB += Array[String](eNB, lon, lat).mkString("|")
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
