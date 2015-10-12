package cn.com.gis.etl.hainan

/**
 * Created by wangxy on 15-10-12.
 */

import com.utils.{ConfigUtils, RedisUtils}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import scala.math._

object HainanGis {

  val propFile = "/config/hninfo.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val LimitNeiDistance = prop.getOrElse("LimitNeiDistance", "0.03").toFloat
  val WeigBase = prop.getOrElse("WeigBase", "1.0").toDouble

  class NEIB_CELL_INFO extends Serializable{
    var lac_ci = "-1"
    var longi = 0.0
    var lati = 0.0
    var sqr_distance = 0.0
    var rxlev = "-1"
  }


  val args_length = 11
  val index_lac = 0
  val index_ci = 1
  val index_RxlevSub = 3

  val neiInfo_length = 5
  val index_neiNum = 2
  val index_neiRxlev = 4
  val index_neiBsic = 6
  val index_neiBcch = 7

  val rindex_lon = 0
  val rindex_lat = 1


  def FindNeibCell(s_longi: Double, s_lati: Double, mr: Array[String], cellinfo: Map[String, String], neiinfo: Map[String, String])
  : ArrayBuffer[NEIB_CELL_INFO] = {
    val neiArr = ArrayBuffer[NEIB_CELL_INFO]()
    var bsic_bcch = 0L
    var neiStr = "-1"

    // 遍历mr中所有临区信息
    for(_idx <- 0 to 5){
      bsic_bcch = mr(index_neiBsic+(neiInfo_length*_idx)).toLong << 16 |
        mr(index_neiBsic+(neiInfo_length*_idx)).toLong
      // 找bsic_bcch相同的基站
      neiStr = neiinfo.getOrElse(bsic_bcch.toString, "-1")
      if(neiStr != "-1"){
        var tmpDistance = 1000.0
        val neiLacCiArr = neiStr.split(",", -1)
        val tmpNeiInfo = new NEIB_CELL_INFO
        var bFlag = false
        // 找最近临区
        neiLacCiArr.foreach(x => {
          val arrLonLat = cellinfo.getOrElse(x, "-1").split(",", -1)
          if(arrLonLat(0) != "-1"){
            val dDistance = pow(tmpNeiInfo.longi.toDouble - s_longi.toDouble, 2) + pow(tmpNeiInfo.lati.toDouble - s_lati.toDouble, 2)
            if(dDistance <= LimitNeiDistance * LimitNeiDistance && dDistance < tmpDistance){
              tmpNeiInfo.lac_ci = x
              tmpNeiInfo.longi = arrLonLat(rindex_lon).toDouble
              tmpNeiInfo.lati = arrLonLat(rindex_lat).toDouble
              tmpNeiInfo.sqr_distance = dDistance
              tmpNeiInfo.rxlev = mr(index_neiRxlev + (neiInfo_length*_idx))
              tmpDistance = dDistance
              bFlag = true
            }
          }
        })
        if(bFlag)
          neiArr += tmpNeiInfo
      }
    }
    neiArr
  }

  def GetLocInfo(s_longi: Double, s_lati: Double, mr: Array[String], neiArr: ArrayBuffer[NEIB_CELL_INFO]): String = {
    var longi = s_longi
    var lati = s_lati

    // 先去除非法点
    var x_ave = longi
    var y_ave = lati
    var x_sqrt = 0.0
    var y_sqrt = 0.0
    neiArr.foreach(x => {
      x_ave += x.longi
      y_ave += x.lati
    })
    x_ave /= (neiArr.length + 1)
    y_ave /= (neiArr.length + 1)

    x_sqrt = pow(longi - x_ave, 2)
    y_sqrt = pow(lati - y_ave, 2)

    neiArr.foreach(x => {
      x_sqrt += pow(x.longi - x_ave, 2)
      y_sqrt += pow(x.lati - y_ave, 2)
    })

    x_sqrt /= neiArr.length
    y_sqrt /= neiArr.length
    x_sqrt = sqrt(x_sqrt)
    y_sqrt = sqrt(y_sqrt)

    var index = 0
    // 过滤非法点
    val fneiArr = neiArr.filter(x => {abs(x.longi - longi) < 2.5 * x_sqrt || abs(x.lati - lati) < 2.5 * y_sqrt})
    var last_neib_rxlev = 0
    if(fneiArr.length > 0)
      last_neib_rxlev = fneiArr(fneiArr.length - 1).rxlev.toInt
    else
      last_neib_rxlev = mr(index_RxlevSub).toInt

    if(mr(index_RxlevSub).toInt < last_neib_rxlev)
      last_neib_rxlev = mr(index_RxlevSub).toInt

    var weig_cell = (mr(index_RxlevSub).toInt - last_neib_rxlev)*1.0/10 + WeigBase
    var weig_sum = 0.0
    if(weig_cell >= WeigBase){
      weig_sum += weig_cell
      longi *= weig_cell
      lati *= weig_cell
    }

    fneiArr.foreach(x => {
      weig_cell = (x.rxlev.toInt - last_neib_rxlev)*1.0/10 + WeigBase
      if(weig_cell >= WeigBase){
        longi += (x.longi * weig_cell)
        lati += (x.lati * weig_cell)
        weig_sum += weig_cell
      }
    })
    longi /= weig_sum
    lati /= weig_sum
    Array[String](longi.toString, lati.toString).mkString(",")
  }

  def mapProcess(in: String, cellinfo: Map[String, String], neiinfo: Map[String, String]): String = {
    var finalstr = "-1"
    val mrArr = in.split(",", -1)
    if(args_length == mrArr.length){
      val cellid = mrArr(index_lac).toLong << 16 | mrArr(index_ci).toLong
      val rInfo = cellinfo.getOrElse(cellid.toString, "-1")
      if(rInfo != "-1"){
        val rArr = rInfo.split(",", -1)
        val neiArr = FindNeibCell(rArr(rindex_lon).toDouble, rArr(rindex_lat).toDouble, mrArr, cellinfo, neiinfo)
        finalstr = GetLocInfo(rArr(rindex_lon).toDouble, rArr(rindex_lat).toDouble, mrArr, neiArr)
      }
    }
    finalstr
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: <in-file> <out-file>")
      System.exit(1)
    }

    val cellinfo = RedisUtils.getResultMap("hnbaseinfo")
    val neiinfo = RedisUtils.getResultMap("hnneiinfo")

    val conf = new SparkConf().setAppName("HainanGis Application")
    val sc = new SparkContext(conf)

    val textRDD = sc.textFile(args(0))

    val result = textRDD.mapPartitions(Iter => Iter.map(x => mapProcess(x, cellinfo, neiinfo)))

    result.saveAsTextFile(args(1))
  }

}
