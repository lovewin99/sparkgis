package cn.com.gis.tools.baise

/**
 * Created by wangxy on 15-10-21.
 */

import cn.com.gis.tools.StaticCellInfo1
import cn.com.gis.utils.tRedisPutMap
import com.utils.RedisUtils

import scala.collection.mutable.{ListBuffer, Map}
import scala.io.Source
import scala.math._

object GenNeiCell1 {
  // 基础字典表 以cell_id为key
  var CellInfo = Map[Int, StaticCellInfo1]()
  // 通过pci_freq 找cell_id 的表
  var Pcifreq2Cells = Map[Long, ListBuffer[Int]]()

  // 最终存redis的两个map
  var Cbaseinfo = Map[String, String]()
  var Cneiinfo = Map[String, String]()        //["cellid,pci_freq", cellid(临区的)]

  def file2St(path : String): Unit ={
    var num = 0
    for (line <- Source.fromFile(path).getLines){
      val strArr : Array[String] = line.split("\t")
      strArr(0)(0) match {
        case '#' => None            // 忽略注释行
        case _ => {
          try {
            if (strArr.length == 11) {
              val c_info = new StaticCellInfo1
              val enbid = strArr(0).toInt
              val cell_in_enb = strArr(1).toInt
              c_info.cellid_ = (enbid << 8) | cell_in_enb
              c_info.longitude_ = strArr(2).toDouble
              c_info.latitude_ = strArr(3).toDouble
              c_info.freq_ = strArr(7).toInt
              c_info.cell_pci_ = strArr(8).toInt
              c_info.in_door_ = strArr(9).toInt
              c_info.azimuth_ = strArr(10).toInt
              val key = (c_info.cell_pci_ << 16) | c_info.freq_
              c_info.pci_freq = key
              CellInfo.put(c_info.cellid_, c_info)

              var arrB = Pcifreq2Cells.getOrElse(key, ListBuffer[Int]())
              arrB += c_info.cellid_
              Pcifreq2Cells.put(key, arrB)

              Cbaseinfo.put(c_info.cellid_.toString, line.toString)
            } else {
              println(line)
              num += 1
            }
          }
          catch {
            case e: NumberFormatException => println(line)
            case unknown: Throwable => println(line)
          }
        }
      }
    }

//    Pcifreq2Cells.foreach(x =>{
//      Cneiinfo.put(x._1.toString, x._2.mkString(","))
//    })

    println("num = " + num)


    // 计算每个区域的临区
    CellInfo.foreach(x => {
      Pcifreq2Cells.foreach(y => {
        var minDistance = 1000.0
        var cell_id = -1
        y._2.foreach(z => {
          z match{
            case x._1 => None // 排除自己
            case _ => {
              // 计算各区域和自己的距离
              val neiCell = CellInfo.getOrElse(z, new StaticCellInfo1)
              // 计算各区域和自己的距离
              val distance : Double = sqrt(pow(neiCell.longitude_ - x._2.longitude_, 2) +
                pow(neiCell.latitude_ - x._2.latitude_, 2))
              if (distance < minDistance){
                minDistance = distance
                cell_id = z
              }
            }
          }
        })
        // 站距大于3公里左右，则抛弃
        if (minDistance <= 0.03){
          cell_id match{
            case -1 => None
            case _ => {
              val Nkey = x._1.toString + ',' + y._1.toString
              val Nvalue = cell_id.toString
              //　建立pci_freq 映射　cell_id的表
              Cneiinfo.put(Nkey, Nvalue)
            }
          }
        }
      })
    })
  }

  def main(args: Array[String]): Unit ={
    if (args.length > 0) {

    }

    file2St("/home/wangxy/data/test1.txt")


    RedisUtils.delTable("baseinfo1")
    RedisUtils.delTable("neiinfo1")

    RedisUtils.putMap2RedisTable("baseinfo1", Cbaseinfo)
    tRedisPutMap.putMap2Redis("neiinfo1", Cneiinfo)
//    Cneiinfo.foreach(println)
//    println("size = "+Cneiinfo.size)

    println("finished")

    //    CellInfo.foreach(e => println("key="+e._1+"     value="+e._2.longitude_))
    //    println("!!!!!!!!!!!!!")
    //    Pcifreq2Cells.foreach(e => println("key="+e._1+"     value="+e._2))
    //    println("!!!!!!!!!!!!!")
    //    Cbaseinfo.foreach(e => println("key="+e._1+"     value="+e._2))
    //    println("!!!!!!!!!!!!!")
    //    Cneiinfo.foreach(e => println("key="+e._1+"     value="+e._2))

  }
}
