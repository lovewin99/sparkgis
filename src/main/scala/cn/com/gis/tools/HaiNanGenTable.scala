package cn.com.gis.tools

import com.utils.{ConfigUtils, RedisUtils}
import scala.collection.mutable.Map
import scala.io.Source
import scala.math._
import scala.collection.mutable.ArrayBuffer

/**
 * Created by wangxy on 15-10-9.
 */
object HaiNanGenTable {

  val propFile = "/config/hninfo.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val LimitNeiDistance = prop.getOrElse("LimitNeiDistance", "0.03").toFloat

  class sCellInfo extends Serializable{
    var lon = 0.0
    var lat = 0.0
    var bcch_bsic = 0L
    var cellid = 0L
  }

  // 基础字典表 以lac_ci为key
  var CellInfo = Map[Long, sCellInfo]()
  // bsic,bcch 找lac_ci 的表
  var BsicBcch2Cells = Map[Long, ArrayBuffer[Long]]()

  // 最终存redis的两个map
  var Cbaseinfo = Map[String, String]()
  var Cneiinfo = Map[String, String]()        //["bsic,bcch", cellid(临区的)]

  // 公参表信息
  val args_length = 34
  val index_ci = 7
  val index_lac = 18
  val index_lon = 12
  val index_lat = 13
  val index_bcch = 15
  val index_bsic = 16


  def file2st(path: String): Unit ={
    var num = 0
    Source.fromFile(path).getLines.foreach(x =>{
      val strArr = x.split(",", -1)
      if(strArr.length == args_length){
        // 构建基础信息表
        val c_info = new sCellInfo
        c_info.lon = strArr(index_lon).toDouble
        c_info.lat = strArr(index_lac).toDouble
        c_info.bcch_bsic = strArr(index_bsic).toLong << 16 | strArr(index_bcch).toLong
        val basekey = strArr(index_lac).toLong << 16 | strArr(index_ci).toLong
        c_info.cellid = basekey
        val basevalue = List[String](strArr(index_lon), strArr(index_lat)).mkString(",")
        Cbaseinfo.put(basekey.toString, basevalue)

        //构建反向映射关系
        val arrB = BsicBcch2Cells.getOrElse(c_info.bcch_bsic, ArrayBuffer[Long]())
        arrB += basekey
        BsicBcch2Cells.put(c_info.bcch_bsic, arrB)
        Cbaseinfo.put(basekey.toString, basevalue)
        CellInfo.put(basekey, c_info)
      }else{
        println(x + "========" + strArr.length)
        num += 1
      }
    })

    BsicBcch2Cells.foreach(x =>{
      Cneiinfo.put(x._1.toString, x._2.mkString(","))
    })

    println("num = " + num)

    // 构建临区信息表
//    CellInfo.foreach(x => {
//      val tmparr = new ArrayBuffer[String]()
//      val arr = BsicBcch2Cells.getOrElse(x._2.bcch_bsic, ArrayBuffer[Long]())
//      arr.foreach(y => {
//        val neiCell = CellInfo.getOrElse(y, new sCellInfo)
//        neiCell.cellid match{
//          case x._1 => None // 排除自己
//          case _ => {
//            // 计算各临区与自己的距离
//            val distance = pow(neiCell.lon - x._2.lon, 2) + pow(neiCell.lat - x._2.lat, 2)
//            if(distance < LimitNeiDistance){
//              tmparr += neiCell.cellid.toString
//            }
//          }
//        }
//      })
//      if(tmparr.length > 1)
//        Cneiinfo.put(x._2.bcch_bsic.toString, tmparr.mkString(","))
//    })
  }

  def main(args: Array[String]): Unit ={
    if (args.length > 0) {

    }

    file2st("/home/wangxy/data/3ginfo.csv")


    RedisUtils.delTable("hnbaseinfo")
    RedisUtils.delTable("hnneiinfo")

//    Cbaseinfo.foreach(println)

    RedisUtils.putMap2RedisTable("hnbaseinfo", Cbaseinfo)
    RedisUtils.putMap2RedisTable("hnneiinfo", Cneiinfo)

  }
}
