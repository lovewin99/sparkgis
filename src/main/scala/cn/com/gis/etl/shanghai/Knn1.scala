package cn.com.gis.etl.shanghai

/**
 * Created by wangxy on 15-10-30.
 */

import org.apache.spark.{SparkContext, SparkConf}
import com.utils.RedisUtils
import scala.collection.mutable.Map

object Knn1 {

  val K = 3

  val MR_Length = 45
  // 服务小区所在字段序号
  val mr_cellid = 17

  val mr_rsrp = 18 // mr 主rsrp

  val mr_neicellid = 22 // 临区cellid 22是第一次出现
  val mr_neirsrp = 23 // 临区rsrp 22是第一次出现

  val neigroup_num= 6        // 临区信息个数

  // 指纹数据长度
  val Finger_length = 18
  // 服务小区所在字段序号
  val Finger_cellid = 4

  val Finger_mainrsrp = 5

  // 指纹库临区cellid 6是第一次出现
  val Finger_neicellid = 6
  // 指纹库临区rsrp 7是第一次出现
  val Finger_neirsrp = 7

  val Finger_lon = 2
  // 经度
  val Finger_lat = 3 // 纬度

  val Table_Name = "FingerInfo"   // 指纹库表名


  def kmaxoffset(kInfo: List[Array[String]], key: Int, value: String): List[Array[String]] = {
    var tmpInfo = kInfo
    if (kInfo.length < K) {
      tmpInfo = tmpInfo ::: List(Array(key.toString, value))
    } else {
      tmpInfo = tmpInfo.sortBy(_(0).toInt).reverse
      if (tmpInfo(0)(0).toInt > key) {
        tmpInfo = tmpInfo.tail
        tmpInfo :::= List(Array(key.toString, value))
      }
    }
    tmpInfo
  }

  //  def mapProcess(in: String, fingerinfo: Map[String, String]): (String, String, String) = {
  def mapProcess(in: String, arrfingerinfo: Array[(String, String)]): (String, String, String) = {
    val fingerinfo = arrfingerinfo.toMap
    var lon = "-1"
    var lat = "-1"
    var err = 0
    val strArr = in.replace("NIL", "").split("\\|", -1)
    if (strArr.length == MR_Length) {
      err = 1
      if (strArr(mr_cellid) != "" && strArr(mr_rsrp) != "") {
        err = 2
        var srcCellid = List[Int]()
        var srcRsrp = List[Int]()
        val mapCellRsrp = Map[Int, Int]()
        val maincellid = if (strArr(mr_cellid) != "") strArr(mr_cellid) else "-1"
        val mainrsrp = if (strArr(mr_rsrp) != "") strArr(mr_rsrp).toInt else -1
        val info = fingerinfo.getOrElse(maincellid, "-1")
        if ("-1" != info && mainrsrp != -1) {
          err = 3
          // 指纹库中找到对应的信息
          var neinum = 0 // mr记录里包含的临区信息个数
          var indexcellid = -1
          var indexrsrp = -1
          for (i <- 0 to neigroup_num - 1) {
            indexcellid = mr_neicellid + i * 4
            indexrsrp = mr_neirsrp + i * 4
            if (strArr(indexcellid) != "" && strArr(indexrsrp) != "") {
              mapCellRsrp.put(strArr(indexcellid).toInt, strArr(indexrsrp).toInt)
            }
          }
          var indexfcell = -1
          var indexfrsrp = -1
          //找出匹配度最高的k条记录
          var kInfo = List[Array[String]]()
          info.split("\\$", -1).map(_.split("\\|", -1)).foreach(x => {
            var lrsrp = List[Int]()
            val fingercr = Map[Int, Int]()
            for (i <- 0 to neigroup_num -1) {
              indexfcell = Finger_neicellid + i * 2
              indexfrsrp = Finger_neirsrp + i * 2
              val ineicellid = if (x(Finger_neicellid) != "") x(Finger_neicellid).toInt else -1
              val ineirsrp = if (x(Finger_neirsrp) != "") x(Finger_neirsrp).toInt else -1
              if(ineicellid != -1 && ineirsrp != -1){
                fingercr.put(ineicellid, ineirsrp)
              }
            }

            val fmrsrp = x(Finger_mainrsrp).toInt
            // 计算电平偏移量
            var tnum = 1
            var offset = (fmrsrp - mainrsrp) * (fmrsrp - mainrsrp)
            mapCellRsrp.foreach(y => {
              val trsrp = fingercr.getOrElse(y._1, -1)
              if(trsrp != -1){
                offset += (trsrp - y._2) * (trsrp - y._2)
                tnum += 1
              }
            })
            offset /= tnum
            kInfo = kmaxoffset(kInfo, offset, x.mkString("|"))
          })

          if (kInfo.length > 0) {
            err = 4
            var lat1 = 0.0
            var lon1 = 0.0
            for (i <- 0 to kInfo.length-1) {
              val tFinfo = kInfo(i)(1).split("\\|")
              lat1 += tFinfo(Finger_lon).toDouble
              lon1 += tFinfo(Finger_lat).toDouble
            }
            lat1 /= kInfo.length
            lon1 /= kInfo.length
            lon = lon1.toString
            lat = lat1.toString
          }
        }
      }
    }
    (lon, lat, err.toString)
  }

  def main(args : Array[String]): Unit ={
    if (args.length != 2) {
      System.err.println("Usage: <in-file> <out-file>")
      System.exit(1)
    }


    val conf = new SparkConf().setAppName("sparkgis Application")
    val sc = new SparkContext(conf)
    val textRDD = sc.textFile(args(0))

    val fingerInfo = RedisUtils.getResultMap(Table_Name).toArray
    val x = sc.broadcast(fingerInfo)

    val result = textRDD.mapPartitions(Iter => {
      Iter.map(e => Knn1.mapProcess(e, x.value))
    })

    result.saveAsTextFile(args(1))
  }
}