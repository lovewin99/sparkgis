package cn.com.gis.etl.shanghai.function

import scala.math._
import scala.collection.mutable.Map

/**
 * Created by wangxy on 15-9-10.
 */
object corr1 {

  val K = 1

  val MR_Length = 45
  // 服务小区所在字段序号
  val mr_cellid = 17

  val mr_rsrp = 18 // mr 主rsrp

  val mr_neicellid = 22
  // 临区cellid 22是第一次出现
  val mr_neirsrp = 23 // 临区rsrp 22是第一次出现

  val neigroup_num = 6 // 临区信息个数

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


  def kmaxoffset(kInfo: List[Array[String]], key: Double, value: String): List[Array[String]] = {
    var tmpInfo = kInfo
    if (kInfo.length < K) {
      tmpInfo = tmpInfo ::: List(Array(key.toString, value))
    } else {
      tmpInfo = tmpInfo.sortBy(_(0).toInt).reverse
      if (tmpInfo(0)(0).toDouble < key) {
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
            for (i <- 0 to neigroup_num - 1) {
              indexfcell = Finger_neicellid + i * 2
              indexfrsrp = Finger_neirsrp + i * 2
              val ineicellid = if (x(Finger_neicellid) != "") x(Finger_neicellid).toInt else -1
              val ineirsrp = if (x(Finger_neirsrp) != "") x(Finger_neirsrp).toInt else -1
              if (ineicellid != -1 && ineirsrp != -1) {
                fingercr.put(ineicellid, ineirsrp)
              }
            }

            val fmrsrp = x(Finger_mainrsrp).toInt

            // 计算相关系数
            var fenzi = fmrsrp * mainrsrp
            var fenmu1 = fmrsrp * fmrsrp
            var fenmu2 = mainrsrp * mainrsrp
            mapCellRsrp.foreach(y =>{
              val trsrp = fingercr.getOrElse(y._1, -1)
              if (trsrp != -1){
                fenzi += trsrp * y._2
                fenmu1 += trsrp * trsrp
                fenmu2 += y._2 * y._2
              }
            })
            val corr = fenzi / sqrt(fenmu1 * fenmu2)
            kInfo = kmaxoffset(kInfo, corr, x.mkString("|"))
          })

          if(kInfo.length > 0){
            err = 4
            val tFinfo = kInfo(0)(1).split("\\|")
            lon = tFinfo(Finger_lon)
            lat = tFinfo(Finger_lat)
          }
        }
      }
    }
    (lon, lat, err.toString)
  }
}

