package cn.com.gis.tools

/**
 * Created by wangxy on 15-10-16.
 */

import cn.com.gis.etl.baise.function.Process1
import com.utils.RedisUtils

import scala.math._
import scala.io.Source
import scala.collection.mutable.Map

object BuildingTable1 {

  // 基础字典表 以cell_id为key
  var CellInfo = Map[Int, StaticCellInfo]()
  // 通过pci_freq 找cell_id 的表
  var Pcifreq2Cells = Map[Int, Vector[Int]]()

  // 最终存redis的两个map
  var Cbaseinfo = Map[String, String]()
  var Cneiinfo = Map[String, String]()        //["cellid,pci_freq", cellid(临区的)]

  val Cbuildinginfo = Map[String, String]()
  val build_length = 9
  val build_lat = 4
  val build_lon = 5

  var x = 11626986.857844816
  var y = 2614746.4238366615

  def file2St(path : String): Unit ={
    var num = 0
    for (line <- Source.fromFile(path).getLines){
      val strArr : Array[String] = line.split("\t")
      strArr(0)(0) match {
        case '#' => None            // 忽略注释行
        case _ => {
          if(strArr.length == 12){
            val c_info = new StaticCellInfo
            val enbid = strArr(0).toInt
            val cell_in_enb = strArr(1).toInt
            c_info.cellid_ = (enbid << 8) | cell_in_enb
            c_info.longitude_ = strArr(2).toDouble
            c_info.latitude_ = strArr(3).toDouble
            c_info.freq_ = strArr(8).toInt
            c_info.cell_pci_ = strArr(9).toInt
            c_info.in_door_ = strArr(10).toInt
            c_info.azimuth_ = strArr(11).toInt
            val key = (c_info.cell_pci_ << 16) | c_info.freq_
            c_info.pci_freq = key
            CellInfo.put(c_info.cellid_, c_info)

            var vec : Vector[Int] = Pcifreq2Cells.getOrElse(key, Vector[Int]())
            vec = c_info.cellid_ +: vec
            Pcifreq2Cells.put(key, vec)

            Cbaseinfo.put(c_info.cellid_.toString, line.toString)
          }else {
            println(line)
            num += 1
          }
        }
      }
    }

    println("num = " + num)
    // 计算每个区域的临区
    CellInfo.foreach(e =>{
      val vec = Pcifreq2Cells.getOrElse(e._2.pci_freq, Vector[Int]())
      vec.size match{
        case 0 => None
        case _ => {
          var minDistance : Double= 1000.0
          var cell_id = -1
          for (i <- 0 to vec.size -1){
            val neiCell : StaticCellInfo = CellInfo.getOrElse(vec(i), new StaticCellInfo)
            neiCell.cellid_ match{
              case e._1 => None     //排除自己
              case _ => {
                // 计算各区域和自己的距离
                val distance : Double = sqrt(pow(neiCell.longitude_ - e._2.longitude_, 2) +
                  pow(neiCell.latitude_ - e._2.latitude_, 2))
                if (distance < minDistance){
                  minDistance = distance
                  cell_id = vec(i)
                }
              }
            }
          }
          // 站距大于3公里左右，则抛弃
          if (minDistance <= 0.03){
            cell_id match{
              case -1 => None
              case _ => {
                val Nkey = e._1.toString + ',' + e._2.pci_freq.toString
                val Nvalue = cell_id.toString
                //　建立pci_freq 映射　cell_id的表
                Cneiinfo.put(Nkey, Nvalue)
              }
            }
          }
        }
      }
    })

  }

  def BuildingInfo(path: String): Unit = {
    var num = 0
    for (line <- Source.fromFile(path).getLines){
      val strArr : Array[String] = line.split(",")
      strArr(0)(0) match {
        case '#' => None            // 忽略注释行
        case _ => {
          if(strArr.length == build_length){
            val lat = strArr(build_lat).toDouble
            val lon = strArr(build_lon).toDouble
            val coo = Process1.lonLat2Mercator(lon, lat)
            val sgX = if ((coo._1 - x) % 100 != 0) ((coo._1 - x) / 100 + 1).toInt else ((coo._1 - x) / 100).toInt
            val sgY = if ((coo._2 - y) % 100 != 0) ((coo._2 - y) / 100 + 1).toInt else ((coo._2 - y) / 100).toInt
            val key = Array[String](sgX.toString, sgY.toString).mkString(",")

            Cbuildinginfo.put(key, "1")
          }else {
            println(line)
            num += 1
          }
        }
      }
    }
    println("num2="+num)
  }

  def main(args: Array[String]): Unit ={
    if (args.length > 0) {

    }

    file2St("/home/wangxy/data/test1.txt")


    RedisUtils.delTable("buildingbaseinfo")
    RedisUtils.delTable("buildingneiinfo")

    RedisUtils.putMap2RedisTable("buildingbaseinfo", Cbaseinfo)
    RedisUtils.putMap2RedisTable("buildingneiinfo", Cneiinfo)

    BuildingInfo("/home/wangxy/data/building.txt")

    RedisUtils.delTable("buildinginfo")
    RedisUtils.putMap2RedisTable("buildinginfo", Cbuildinginfo)

    //    CellInfo.foreach(e => println("key="+e._1+"     value="+e._2.longitude_))
    //    println("!!!!!!!!!!!!!")
    //    Pcifreq2Cells.foreach(e => println("key="+e._1+"     value="+e._2))
    //    println("!!!!!!!!!!!!!")
    //    Cbaseinfo.foreach(e => println("key="+e._1+"     value="+e._2))
    //    println("!!!!!!!!!!!!!")
    //    Cneiinfo.foreach(e => println("key="+e._1+"     value="+e._2))

  }

}
