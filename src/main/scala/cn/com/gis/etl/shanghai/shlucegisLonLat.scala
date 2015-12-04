package cn.com.gis.etl.shanghai

import java.text.SimpleDateFormat

import com.utils.ConfigUtils
import org.apache.spark.{SparkContext, SparkConf}

import scala.math._
import scala.collection.mutable.ArrayBuffer
import cn.com.gis.etl.shanghai.function.shfinger1LonLat
import com.utils.RedisUtils

/**
 * Created by wangxy on 15-11-26.
 */
object shlucegisLonLat {
  val propFile = "/config/shanghai.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val finger_line_max_num = prop.getOrElse("FINGER_LINE_MAX_NUM", "12").toInt
  val grip_size = prop.getOrElse("GRID_SIZE", "25").toInt
  val rssi_uplimit = prop.getOrElse("RSSI_UPLIMIT", "-40").toInt
  val rssi_downlimit = prop.getOrElse("RSSI_DOWNLIMIT", "-100").toInt

  val combineinfo_num = prop.getOrElse("COMBINEINFO_NUM", "2").toInt
  val combineinfo_timelimit = prop.getOrElse("COMBINEINFO_TIMELIMIT", "20000").toInt

  // 指纹库名称
  val bsLib_name = prop.getOrElse("bsLib_name", "shbaselib1")
  val neiLib_name = prop.getOrElse("neiLib_name", "shneilib1")
  val fingerlib_name = prop.getOrElse("fingerlib_name", "shfingerlib1")
  val tsmrsrplib_name = prop.getOrElse("rsrpLib_name", "tsmrsrplib1")

  // 站距 m
  val dis_limit = 3000.0

  //数据最小长度(无临区情况)
  val mrmin_length = 7
  val imsi_index = 0
  val time_index = 1
  val eNB_index = 2
  val ta_index = 3
  val rsrp_index = 4
  // 为计算误差加路测数据经纬度
  val lon_index = 5
  val lat_index = 6


  // 临区信息相对位置
  // 临区信息长度
  val nei_length = 2
  val pci_freq_index = 0
  val neirsrp_index = 1

  // 路测格式
  val luce_length = 11
  val lcsampling_index = 0
  val lctime_index = 1
  val lclon_index = 2
  val lclat_index = 3
  val lcfreq_index = 6
  val lcpci_index = 7
  val lcta_index = 8
  val lcrsrp_index = 9
  val lcismcell_index = 10

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

  // Array[String](imsi, time, eNB/pci_freq, ta, rsrp, ismcell)
  def lcMap(in: String, neiInfo: Map[String, String]): (String, Array[String]) = {
    val strArr = in.split(",", -1)
    if(luce_length == strArr.length){
      val sampling = strArr(lcsampling_index)
      val time = strArr(lctime_index)
      val lon = strArr(lclon_index).toDouble
      val lat = strArr(lclat_index).toDouble
      val pcifreq = strArr(lcpci_index) + "|" + strArr(lcfreq_index)
      val ta = strArr(lcta_index)
      val rsrp = strArr(lcrsrp_index)
      val ismcell = strArr(lcismcell_index)
      val (nlon, nlat) = wgsTOgcj(lon, lat)
      if("1" == ismcell){
        neiInfo.get(pcifreq) match{
          case None => ("", Array[String]())
          case Some(info) => {
            var tmpd = 1000000.0
            var neNB = ""
            info.split(",", -1).map(_.split("\\|", -1)).foreach{
              case Array(teNB, tlon, tlat) => {
                // 找最近站 并小于规定距离
                val d = calc_distance(tlon.toDouble, tlat.toDouble, lon, lat)
                if(d < dis_limit && d < tmpd){
                  neNB = teNB
                  tmpd = d
                }
              }
            }
            if(neNB != ""){
              (sampling, Array[String]("1", time, neNB, rsrp, ta, ismcell, nlon.toString, nlat.toString))
            }else{
              ("", Array[String]())
            }
          }
        }
      }else {
        (sampling, Array[String]("1", time, pcifreq, rsrp, ta, ismcell, nlon.toString, nlat.toString))
      }
    }else{
      ("", Array[String]())
    }
  }

  // Array[String](imsi, time, eNB/pci_freq, ta, rsrp, ismcell)
  def lcReduce(sampling: String, Iter: Iterable[Array[String]]): String = {
    val dataArr = Iter.toArray.sortBy(_(5)).reverse
    if("1" == dataArr.head(5)){
      val fdataArr = ArrayBuffer[String]()
      fdataArr ++= Array[String](dataArr.head(0), dataArr.head(1), dataArr.head(2), dataArr.head(4), dataArr.head(3), dataArr.head(6), dataArr.head(7))
      for(i <- 1 to dataArr.length - 1){
        fdataArr += dataArr(i).slice(2, 4).mkString("$")
      }
      fdataArr.mkString(",")
    }else{
      ""
    }
  }

  // 计算两经纬度距离(单位 m)
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

  // (用户, (公共信息, 定位信息))
  // 公共信息: 时间,经度,纬度 定位信息: 多个纹线   纹线: 标识,ta,ismain,rxlevsub
  def inMapProcess(in: String, neiInfo: Map[String, String], rsrpInfo: Map[String, String], mInfo: Map[String, String]):
  (String, (Array[String], ArrayBuffer[ArrayBuffer[String]])) = {
    val strArr = in.split(",", -1)
    if(strArr.length >= mrmin_length){
      var fingerArr =  ArrayBuffer[ArrayBuffer[String]]()
      val imsi = strArr(imsi_index)
      val time = strArr(time_index)
      val eNB = strArr(eNB_index)
      val ta = strArr(ta_index)
      val mrsrp = strArr(rsrp_index)
      val lon = strArr(lon_index)
      val lat = strArr(lat_index)
      //      val fmrsrp = rsrpInfo.getOrElse(mrsrp, "")
      if(eNB != "" && mrsrp != "" && time != ""){
        // 将主服务小区纹线信息装入
        fingerArr += ArrayBuffer[String](eNB, ta, "1", mrsrp)
      }
      // 处理临区信息
      for(i <- 0 to (strArr.length - mrmin_length - 1)){
        mInfo.get(eNB) match{
          case None => None
          case Some(strlonlat) => {
            val lonlat = strlonlat.split(",", -1)
            val neiArr = strArr(mrmin_length + i).split("\\$", -1)
            val neiPci_freq = neiArr(pci_freq_index)
            val neiRsrp = neiArr(neirsrp_index)
            neiInfo.get(neiPci_freq) match{
              case None => None
              case Some(info) => {
                var tmpd = 1000000.0
                var neNB = ""
                info.split(",", -1).map(_.split("\\|", -1)).foreach{
                  case Array(teNB, tlon, tlat) => {
                    // 找最近站 并小于规定距离
                    val d = calc_distance(tlon.toDouble, tlat.toDouble, lonlat(0).toDouble, lonlat(1).toDouble)
                    if(d < dis_limit && d < tmpd){
                      neNB = teNB
                      tmpd = d
                    }
                  }
                }
                if(neNB != ""){
                  fingerArr += ArrayBuffer[String](neNB, ta, "0", neiRsrp)
                }
              }
            }
          }
        }
      }
      if(fingerArr.length != 0){
        (imsi, (Array[String](time, lon, lat), fingerArr))
      }else{
        ("", (Array[String](), ArrayBuffer[ArrayBuffer[String]]()))
      }
    }else{
      ("", (Array[String](), ArrayBuffer[ArrayBuffer[String]]()))
    }
  }

  // (用户, (公共信息, 定位信息))
  // 公共信息: 时间,栅格,采样点  定位信息: 多个纹线   纹线: 标识,ta,ismain,rxlevsub
  def CombineUserInfo(imsi: String, Iter: Iterable[(Array[String], ArrayBuffer[ArrayBuffer[String]])], fingerLib: Map[String, String]): String = {
    val fingerLib1 = fingerLib.map{x =>
      val arr = x._2.split("\\$", -1).map{_.split(",", -1)}
      (x._1, arr)
    }.toArray

    if(combineinfo_num > 1){
      var fArr = ArrayBuffer[(Array[String], ArrayBuffer[ArrayBuffer[String]])]()
      val sortArr = Iter.toArray.sortBy(_._1(0))
      var tIndex = 0
      while((tIndex + combineinfo_num) <= sortArr.length){
        val tArr = sortArr.slice(tIndex, tIndex+combineinfo_num)
        val sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS")
        val firsttime = sdf.parse(tArr(0)._1(0)).getTime
        val lasttime = sdf.parse(tArr(combineinfo_num-1)._1(0)).getTime
        if((lasttime-firsttime)>combineinfo_timelimit){
          tIndex += 1
          fArr += ((Array[String](tArr(0)._1(0)), tArr(0)._2))
        }else{
          var fcombineinfo = ArrayBuffer[ArrayBuffer[String]]()
          val fingerInfo = ArrayBuffer[ArrayBuffer[String]]()
          tIndex += combineinfo_num
          tArr.foreach{
            case (comInfo, fInfo) => {
              fingerInfo ++= fInfo
            }
          }
          //纹线: 标识,ta,ismain,rxlevsub
          fingerInfo.groupBy(_.head).foreach{v =>
            val sum = v._2.length
            val mRsrp = v._2./:(0) {(x, y) => x + y(3).toInt} / sum
            // 如果有主小区存在 取主小区标识 (flag, ta, ismcell)
            val ismcell = v._2.sortBy(_(2)).reverse.head(2)
            val ta = v._2.head(1)
            fcombineinfo += ArrayBuffer[String](v._1, ta, ismcell, mRsrp.toString)
          }
          fArr += ((tArr(combineinfo_num-1)._1, fcombineinfo))
        }
      }
      fArr ++= sortArr.slice(tIndex, sortArr.length)
      shfinger1LonLat.location(imsi, fArr.toArray, fingerLib1)
    }else{
      shfinger1LonLat.location(imsi, Iter.toArray, fingerLib1)
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: <in-file> <out-file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("shlucegislonlat")
    val sc = new SparkContext(conf)

    val bsinfo = RedisUtils.getResultMap(bsLib_name)
    val neiinfo = RedisUtils.getResultMap(neiLib_name)
    val fingerinfo = RedisUtils.getResultMap(fingerlib_name)
    val rsrpinfo = RedisUtils.getResultMap(tsmrsrplib_name)

    val bslib = sc.broadcast(bsinfo)
    val neilib = sc.broadcast(neiinfo)
    val fingerlib= sc.broadcast(fingerinfo)
    val rsrplib = sc.broadcast(rsrpinfo)

    val textRdd = sc.textFile(args(0))
    val result1 = textRdd.map{x => lcMap(x, neilib.value)}.filter(_._1 != "").groupByKey().map{y => lcReduce(y._1, y._2)}.filter(_ != "")//
    val result = result1.map{x => inMapProcess(x, neilib.value, rsrplib.value, bslib.value)}.filter(_._1 != "").groupByKey().map{y => CombineUserInfo(y._1, y._2, fingerlib.value)}
    result.saveAsTextFile(args(1))
  }
}
