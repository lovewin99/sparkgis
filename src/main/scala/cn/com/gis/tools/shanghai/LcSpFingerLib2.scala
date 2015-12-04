package cn.com.gis.tools.shanghai

import cn.com.gis.utils.tRedisPutMap
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.collection.immutable.{Map =>imMap}
import scala.io.Source
import scala.math._
import com.utils.{RedisUtils, ConfigUtils}
import cn.com.gis.tools.cooTranslate.lonlat2gkp

/**
 * Created by wangxy on 15-11-30.
 */
object LcSpFingerLib2 {

  val propFile = "/config/shanghai.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val finger_line_max_num = prop.getOrElse("FINGER_LINE_MAX_NUM", "12").toInt
  val grip_size = prop.getOrElse("GRID_SIZE", "25").toInt
  val rssi_uplimit = prop.getOrElse("RSSI_UPLIMIT", "-40").toInt
  val rssi_downlimit = prop.getOrElse("RSSI_DOWNLIMIT", "-100").toInt

  // 指纹库名称
  val bsLib_name = prop.getOrElse("bsLib_name", "shbaselib1")
  val neiLib_name = prop.getOrElse("neiLib_name", "shneilib1")
  val fingerlib_name = prop.getOrElse("fingerlib_name", "shfingerlib1")

  // 站距 m
  val dis_limit = 3000.0

  // 路测数据格式
  val luce_length = 12
  val lclon_index = 2
  val lclat_index = 3
  val lcfreq_index = 6
  val lcpci_index = 7
  val lcta_index = 8
  val lcrsrp_index = 10
  val lcismcell_index = 11

  // 扫频数据格式
  val saopin_length = 15
  val splon_index = 1
  val splat_index = 2
  val spfreq_index = 3
  val sppci_index = 4
  val sprsrp_index = 7

  val isspinfo = "0"
  val isluceinfo = "1"

  val ee = 0.00669342162296594323
  val aM = 6378245.0

  // 中央经线
  val medien = 121.0
//  val x = 3420252.5173054
//  val y = 556413.619816685
  val x = 3420252.5173054
  val y = 556413.619816685

  //  val x = 3420252.0
//  val y = 556413.0

  // 划分栅格
  def gkp2sg(xsrc: Double, ysrc: Double): (Double, Double) = {
    val xdes = x + rint((xsrc - x) / grip_size) * grip_size
    val ydes = y + rint((ysrc - y) / grip_size) * grip_size
    (xdes, ydes)
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
//  def wgsTOgcj(wgLon : Double, wgLat : Double) : (Double, Double)={
//    if(outOfChina(wgLon, wgLat)){
//      (wgLon, wgLat)
//    }else{
//      var dLat = transformLat(wgLon - 105.0, wgLat - 35.0)
//      var dLon = transformLon(wgLon - 105.0, wgLat - 35.0)
//      val radLat = wgLat / 180.0 * Pi
//      var magic = sin(radLat)
//      magic = 1 - ee * magic * magic
//      val sqrtMagic = sqrt(magic)
//      dLat = (dLat * 180.0) / ((aM * (1 - ee)) / (magic * sqrtMagic) * Pi)
//      dLon = (dLon * 180.0) / (aM / sqrtMagic * cos(radLat) * Pi)
//      val mgLat = wgLat + dLat
//      val mgLon = wgLon + dLon
//      (mgLon, mgLat)
//    }
//  }

  //*******************************************************/
  def wgsTOgcj(wgLon : Double, wgLat : Double) : (Double, Double)={
    (wgLon, wgLat)
  }

  //经纬度转墨卡托
  def lonLat2Mercator(lon : Double, lat : Double) : (Double, Double) = {
    val x = lon * 20037508.342789 / 180
    var y = log(tan((90+lat)*Pi/360)) / (Pi / 180)
    y = y * 20037508.34789 / 180
    (x, y)
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

  // 电频值在范围内
  def rejectByRssi(info: String): Boolean = {
    info != "" && info.toDouble >= rssi_downlimit && info.toDouble <= rssi_uplimit
  }

  // key:(sgx, sgy) value:(eNB, ta, ismcell, rsrp, isluce)
  def luceMap(in: String, neiMap: imMap[String, String]): (String, (String, String, String, String, String)) = {
    val strArr = in.split(",", -1)
    if(luce_length == strArr.length){
      val lon = strArr(lclon_index).toDouble
      val lat = strArr(lclat_index).toDouble
      val (hlon, hlat) = wgsTOgcj(lon, lat)
      val pci_freq = strArr(lcpci_index) + "|" + strArr(lcfreq_index)
      val ta = strArr(lcta_index)
      val rsrp = strArr(lcrsrp_index)
      val ismcell = strArr(lcismcell_index)
      if("" == rsrp || rsrp.toInt < -140 || rsrp.toInt >0){
        // 如果电频值为空或者超限 则去除这条数据
        ("", ("", "", "", "", ""))
      }else{
        var eNB = ""
        neiMap.get(pci_freq) match{
          case None => None
          case Some(info) => {
            var tmpd = 1000000.0
            info.split(",", -1).map(_.split("\\|", -1)).foreach{
              case Array(teNB, tlon, tlat) => {
                // 找最近站 并小于规定距离
                val d = calc_distance(tlon.toDouble, tlat.toDouble, hlon, hlat)
                if(d < dis_limit && d < tmpd){
                  eNB = teNB
                  tmpd = d
                }
              }
            }
          }
        }
        if("" == eNB){
          // 如果没有取到合适的eNB则过滤此条纹线
          ("", ("", "", "", "", ""))
        }else{
          val gxy = lonlat2gkp(hlon, hlat, medien)
//          val sgx = rint(gxy._1 / grip_size).toLong
//          val sgy = rint(gxy._2 / grip_size).toLong
//          val sg = sgx + "|" +sgy
          val sgxy = gkp2sg(gxy._1, gxy._2)
          val sg = sgxy._1 + "|" +sgxy._2
//          val (mx, my) = lonLat2Mercator(hlon, hlat)
//          val sgx = rint(mx / grip_size).toLong
//          val sgy = rint(my / grip_size).toLong
//          val sg = sgx + "|" +sgy
          // key:(sgx, sgy) value:(eNB, ta, ismcell, rsrp)

          (sg, (eNB, ta, ismcell, rsrp, isluceinfo))
        }
      }
    }else{
      ("", ("", "", "", "", ""))
    }
  }

  // key:(sgx, sgy) value:(eNB, ta, ismcell, rsrp, isluce)
  def spMap(in: String, neiMap: imMap[String, String]): (String, (String, String, String, String, String)) = {
    val strArr = in.split(",", -1)
    if(saopin_length == strArr.length){
      val lon = strArr(splon_index).toDouble
      val lat = strArr(splat_index).toDouble
      val (hlon, hlat) = wgsTOgcj(lon, lat)
      val pci_freq = strArr(sppci_index) + "|" + strArr(spfreq_index)
      val ta = "0"
      val rsrp = if(strArr(sprsrp_index) != "") strArr(sprsrp_index) else ""
      val ismcell = "0"
      var tmpd = 1000000.0
      if("" == rsrp || rsrp.toDouble < -140.0 || rsrp.toDouble >0.0){
        // 如果电频值为空或者超限 则去除这条数据
        ("", ("", "", "", "", ""))
      }else{
        var eNB = ""
        neiMap.get(pci_freq) match{
          case None => None
          case Some(info) => {
            //var tmpd = 1000000.0
            info.split(",", -1).map(_.split("\\|", -1)).foreach{
              case Array(teNB, tlon, tlat) => {
                // 找最近站 并小于规定距离
                val d = calc_distance(tlon.toDouble, tlat.toDouble, hlon, hlat)
                if(d < dis_limit && d < tmpd){
                  eNB = teNB
                  tmpd = d
                }
              }
            }
          }
        }
        if("" == eNB){
          // 如果没有取到合适的eNB则过滤此条纹线
          ("", ("", "", "", "", ""))
        }else{
          val gxy = lonlat2gkp(hlon, hlat, medien)
//          val sgx = rint(gxy._1 / grip_size).toLong
//          val sgy = rint(gxy._2 / grip_size).toLong
//          val sg = sgx + "|" +sgy
          val sgxy = gkp2sg(gxy._1, gxy._2)
          val sg = sgxy._1 + "|" +sgxy._2
//          val (mx, my) = lonLat2Mercator(hlon, hlat)
//          val sgx = rint(mx / grip_size).toLong
//          val sgy = rint(my / grip_size).toLong
//          val sg = sgx + "|" +sgy
          // key:(sgx, sgy) value:(eNB, ta, ismcell, rsrp)

          (sg, (eNB, ta, ismcell, rsrp, isspinfo))
        }
      }
    }else{
      ("", ("", "", "", "", ""))
    }
  }

  // key:(sgx, sgy) value:(eNB, ta, ismcell, rsrp, isluce)
  def zReduce(key: String, Iter: Iterable[(String, String, String, String, String)], fmap: Map[String, String]): Unit = {
    // 将路测数据和扫频数据分别合并去重
    val lcSpMap = Iter.toArray.groupBy(_._5).map{v1 =>
      // v1._2: Array[(eNB, ta, ismcell, rsrp, isluce)] 将路测和扫频的路测信息分开
      val fingerlineArr = v1._2.groupBy(_._1).map{v2 =>
        // v2._2: Array[(eNB, ta, ismcell, rsrp, isluce)] 将路测或扫频的信息按照相同eNB合并
        val sum = v2._2.length
        val mRsrp = v2._2./:(0.0) {(x, y) => x + y._4.toDouble} / sum
        // 如果有主小区存在 取主小区标识 (flag, ta, ismcell)
        val ismcell = v2._2.sortBy(_._3).reverse.head._3
        val isluce = v2._2.head._5
        val ta = v2._2.head._2
        (v2._1, ta, ismcell, mRsrp.toString, isluce, sum)
      }.toArray
      (v1._1, fingerlineArr)
    }
    lcSpMap.size match{
      case 1 => {
        // 只存扫频数据 数据直接存
        var fArr = ArrayBuffer[Array[String]]()
        if(lcSpMap.head._1 == isspinfo){
          val value = lcSpMap.head._2.foreach{
            case (eNB, ta, ismcell, rsrp, isluce, sum) => {
              var nrsrp = rsrp.toDouble
              if(lcSpMap.head._1 == isspinfo)
                nrsrp -= 8.0
              if(rejectByRssi(nrsrp.toString)){
                fArr += Array[String](eNB, ta, ismcell, nrsrp.toString, sum.toString)
              }
            }
          }
          if(fArr.length != 0){
            // 优先保留主服务小区的, 再保留临区的
            // Array(标识(bcch|bsic), ta, ismell, rxlevsub, sum)
            val fingerInfo = fArr.filter(_(2) == "1")
            val neiInfo = fArr.filter(_(2) == "0")
            if (fingerInfo.length >= finger_line_max_num) {
              val fstr = fingerInfo.sortBy(_(3).toDouble).reverse.slice(0, finger_line_max_num).map(_.mkString(",")).mkString("$")
              fmap.put(key, fstr)
            } else {
              fingerInfo ++= neiInfo
              val fstr = fingerInfo.sortBy(_(3).toDouble).reverse.slice(0, finger_line_max_num).map(_.mkString(",")).mkString("$")
              fmap.put(key, fstr)
            }
          }
        }else{
//          println(s"key = $key")
        }
      }
      case 2 => {
        // 根据路测修正扫频信息
        var fArr = ArrayBuffer[Array[String]]()
        var vxiuzheng = 0.0
        var num = 0
        val lcArr = lcSpMap.getOrElse(isluceinfo, Array())
        var spArr = lcSpMap.getOrElse(isspinfo, Array())
        var spExitArr = ArrayBuffer[String]()
        if(lcArr.length != 0 || spArr.length != 0){
          lcArr.foreach{
            case (lceNB, lcta, lcismcell, lcrsrp, lcisluce, sum) =>{
              if(rejectByRssi(lcrsrp)){
                fArr += Array[String](lceNB, lcta, lcismcell, lcrsrp.toDouble.toString, sum.toString)
              }
              spArr.foreach{
                case (speNB, spta, spismcell, sprsrp, spisluce, sum1) =>{
                  if(rejectByRssi(lcrsrp)){
                    if(lceNB == speNB){
                      vxiuzheng += abs(lcrsrp.toDouble - sprsrp.toDouble)
                      num += 1
                      spExitArr += speNB
                    }
                  }else{
                    if(lceNB == speNB){
                      vxiuzheng += abs(lcrsrp.toDouble - sprsrp.toDouble)
                      num += 1
                    }
                  }
                }
              }
            }
          }

          val mxiuzheng = vxiuzheng / num
          spExitArr.foreach(x => spArr = spArr.filter(_._1 != x))

          spArr.foreach{
            case (speNB, spta, spismcell, sprsrp, spisluce, sum) => {
              val frsrp = sprsrp.toDouble - mxiuzheng
              if(rejectByRssi(frsrp.toString)){
                fArr += Array[String](speNB, spta, spismcell, frsrp.toString, sum.toString)
              }
            }
          }
          if(fArr.length != 0){
            // 优先保留主服务小区的, 再保留临区的
            // Array(标识(bcch|bsic), ta, ismell, rxlevsub, sum)
            val fingerInfo = fArr.filter(_(2) == "1")
            val neiInfo = fArr.filter(_(2) == "0")
            if (fingerInfo.length >= finger_line_max_num) {
              val fstr = fingerInfo.sortBy(_(3).toDouble).reverse.slice(0, finger_line_max_num).map(_.mkString(",")).mkString("$")
              fmap.put(key, fstr)
            } else {
              fingerInfo ++= neiInfo
              val fstr = fingerInfo.sortBy(_(3).toDouble).reverse.slice(0, finger_line_max_num).map(_.mkString(",")).mkString("$")
              fmap.put(key, fstr)
            }
          }
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: <luce-file> <saopin-file>")
      System.exit(1)
    }

    RedisUtils.delTable(fingerlib_name)
    val neiinfo = RedisUtils.getResultMap(neiLib_name)

    val conf = new SparkConf().setAppName("LcSpFingerLib1")
    val sc = new SparkContext(conf)

    val nei = sc.broadcast(neiinfo)

    val luceRDD = sc.textFile(args(0)).map{x => luceMap(x, nei.value)}.filter(_._1 != "")
    val spRDD = sc.textFile(args(1)).map{x => spMap(x, nei.value)}.filter(_._1 != "")

    luceRDD.union(spRDD).groupByKey().foreachPartition{Iter =>
      val fmap = Map[String, String]()
      Iter.foreach(x => zReduce(x._1, x._2, fmap))
      tRedisPutMap.putMap2Redis(fingerlib_name, fmap.toMap)
    }

  }
}
