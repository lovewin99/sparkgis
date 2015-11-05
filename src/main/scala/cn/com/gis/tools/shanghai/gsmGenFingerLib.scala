package cn.com.gis.tools.shanghai

/**
 * Created by wangxy on 15-11-2.
 */

import cn.com.gis.utils.tRedisPutMap
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.{ArrayBuffer, Map, ListBuffer}

import scala.math._

object gsmGenFingerLib {

  // 指纹库名称
  val Finget_name = "gsmFingerLib"

  // 指纹数据长度
  val Finger_length = 11
  val Sampling_index = 0
  val lon_index = 2
  val lat_index = 3
  val lac_index = 4
  val ci_index = 5
  val bcch_index = 6
  val bsic_index = 7
  val relevsub_index = 9
  val ismcell_index = 10

  // 最大临区个数
  val nei_maxnum = 6


  val Table_Name = "gsmFingerInfo" // 指纹库表名

  //经纬度转墨卡托
  def lonLat2Mercator(lon : Double, lat : Double) : (Double, Double) = {
    val x = lon * 20037508.342789 / 180
    var y = log(tan((90+lat)*Pi/360)) / (Pi / 180)
    y = y * 20037508.34789 / 180
    (x, y)
  }

  // 输出(采样点, 整条数据)
  def inMapProcess(in: String): (String, Array[String]) = {
    println(in)
    val strArr = in.split(",", -1)
    if(Finger_length == strArr.length){
      (strArr(Sampling_index), strArr)
    }else {
      ("-1", Array[String]())
    }
  }

  // 输出((栅格, 主小区id), 拼好的数组)
  // 2g主小区id为lac,ci
  // value (主小区电频值,ArrayBuffer[(bcch|bsic,rsrp)])
  def inReduceProcess(sampling: String, Iter: Iterable[Array[String]]): ((String, String), (Int, ArrayBuffer[(String, Int)])) = {
    var key = ("-1","-1")
    var neiInfo = ArrayBuffer[(String, Int)]()
    var mainRsrp = -1
    if(sampling != "-1"){
      val value = ArrayBuffer[(String, Int)]()
//      var isFirst = true
      Iter.foreach(x => {
        if("1" == x(ismcell_index)){
//          isFirst = false
          val coo = lonLat2Mercator(x(lon_index).toDouble, x(lat_index).toDouble)
          val sg = (coo._1 / 50).toInt + "|" + (coo._2 / 50).toInt
          val cell_id = x(lac_index) + "|" + x(ci_index)
          key = (sg, cell_id)
          mainRsrp = x(relevsub_index).toInt + 141
          neiInfo += ((x(bcch_index)+"|"+x(bsic_index), x(relevsub_index).toInt + 141))
        } else{
          neiInfo += ((x(bcch_index)+"|"+x(bsic_index), x(relevsub_index).toInt + 141))
        }
      })
    }
    (key, (mainRsrp, neiInfo))
  }

  // redis表中字段结构: key:主小区id value: 栅格,电频,临区(bcch|bsic, 电频)*6
  // (主小区id, 整条记录)
  def tjReduceProcess(key: (String, String), Iter: Iterable[(Int, ArrayBuffer[(String, Int)])]): (String, String) = {
    val sg = key._1
    val maincell_id = key._2
    var rsrp = 0
    var c = ArrayBuffer[(String, Int)]()
    Iter.foreach(x => {
      rsrp += x._1
      c ++= x._2
    })
    rsrp /= Iter.size

    var neiInfo = ArrayBuffer[(String, Int, Int)]()
    var tBhBc = ""
    var tRsrp = 0
    var tCount = 0
    c.sortBy(_._1).foreach(x => {
      if(x._1 == tBhBc || "" == tBhBc){
        tBhBc = x._1
        tRsrp += x._2
        tCount += 1
      } else{
        neiInfo += ((tBhBc, tRsrp/tCount, tCount))
        tBhBc = x._1
        tRsrp = x._2
        tCount = 1
      }
    })

    if(tBhBc != "")
      neiInfo += ((tBhBc, tRsrp/tCount, tCount))

    val neiArr = neiInfo.sortBy(_._3).reverseMap(x => x._1+","+x._2).slice(0,6)
    var neiList = ListBuffer[String]()
    neiList ++= neiArr
    if(neiList.length < nei_maxnum){
      for(i <- 1 to (nei_maxnum - neiList.length)){
        neiList += ","
      }
    }
    val value = Array(sg, rsrp, neiList.mkString(",")).mkString(",")
    (maincell_id, value)
  }

  def cbReduceProcess(key: String, Iter: Iterable[String], fmap: Map[String, String]): Unit = {
    fmap.put(key, Iter.mkString("$"))
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: <in-file> <out-file>")
      System.exit(1)
    }

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val textRDD = sc.textFile(args(0))

    tRedisPutMap.deltable(Finget_name)

    textRDD.mapPartitions(Iter=> Iter.map(inMapProcess)).groupByKey().map(x => inReduceProcess(x._1, x._2)).groupByKey().map(x => tjReduceProcess(x._1, x._2))
      .groupByKey().foreachPartition(Iter => {
      val fmap = Map[String, String]()
      Iter.foreach(x => cbReduceProcess(x._1, x._2, fmap))
      tRedisPutMap.putMap2Redis(Finget_name, fmap)
//      fmap.foreach(println)
    })
  }
}
