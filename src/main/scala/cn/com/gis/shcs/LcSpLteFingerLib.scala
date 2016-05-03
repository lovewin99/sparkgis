package cn.com.gis.shcs


import cn.com.gis.utils.tRedisPutMap
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.collection.immutable.{Map =>imMap}
import scala.math._
import com.utils.{RedisUtils, ConfigUtils}

/**
 * 根据扫频和marine数据生成lte指纹库
 * spark-submit --master spark://cloud138:7077 --total-executor-cores 50 --executor-memory 15g --jars  jedis-2.1.0.jar,commons-pool-1.5.6.jar,redisclient_2.10-2.12.jar,commons-pool-1.5.6.jar --driver-class-path redisclient_2.10-2.12.jar:commons-pool-1.5.6.jar --class cn.com.gis.shcs.LcSpLteFingerLib sparkgis_2.10-1.0.jar wxy/sh0503/gsm.csv
 * Created by wangxy on 16-5-3.
 */
object LcSpLteFingerLib {
  val propFile = "/config/shanghai.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val finger_line_max_num = prop.getOrElse("FINGER_LINE_MAX_NUM", "12").toInt
  val grip_size = prop.getOrElse("GRID_SIZE", "25").toInt
  val rssi_uplimit = prop.getOrElse("RSSI_UPLIMIT", "-40").toInt
  val rssi_downlimit = prop.getOrElse("RSSI_DOWNLIMIT", "-100").toInt

  // 指纹库名称
//  val bsLib_name = prop.getOrElse("bsLib_name", "shbaselib1")
//  val neiLib_name = prop.getOrElse("neiLib_name", "shneilib1")
//  val fingerlib_name = prop.getOrElse("fingerlib_name", "shfingerlib1")
  val fingerlib_name = "shlte0503"

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
  val saopin_length = 11
  val splon_index = 1
  val splat_index = 2
  val spfreq_index = 3
  val sppci_index = 4
  val sprsrp_index = 7

  val isspinfo = "0"
  val isluceinfo = "1"

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
    info != "" && info.toDouble.toInt >= rssi_downlimit && info.toDouble.toInt <= rssi_uplimit
  }

  // key:(sgx, sgy) value:(eNB, ta, ismcell, rsrp, isluce)
  def luceMap(in: String): (String, (String, String, String, String, String)) = {
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
        val (mx, my) = lonLat2Mercator(hlon, hlat)
        val sgx = rint(mx / grip_size).toLong
        val sgy = rint(my / grip_size).toLong
        val sg = sgx + "|" +sgy
        // key:(sgx, sgy) value:(pci_freq, ta, ismcell, rsrp)
        (sg, (pci_freq, ta, ismcell, rsrp, isluceinfo))
      }
    }else{
      ("", ("", "", "", "", ""))
    }
  }

  // key:(sgx, sgy) value:(eNB, ta, ismcell, rsrp, isluce)
  def spMap(in: String): (String, (String, String, String, String, String)) = {
    val strArr = in.split(",", -1)
    if(saopin_length == strArr.length){
      val lon = strArr(splon_index).toDouble
      val lat = strArr(splat_index).toDouble
      val (hlon, hlat) = wgsTOgcj(lon, lat)
      val pci_freq = strArr(sppci_index) + "|" + strArr(spfreq_index)
      val ta = "0"
      val rsrp = if(strArr(sprsrp_index) != "") strArr(sprsrp_index).toDouble.toInt.toString else ""
      val ismcell = "0"
      if("" == rsrp || rsrp.toInt < -140 || rsrp.toInt >0){
        // 如果电频值为空或者超限 则去除这条数据
        ("", ("", "", "", "", ""))
      }else{
        val (mx, my) = lonLat2Mercator(hlon, hlat)
        val sgx = rint(mx / grip_size).toLong
        val sgy = rint(my / grip_size).toLong
        val sg = sgx + "|" +sgy
        // key:(sgx, sgy) value:(eNB, ta, ismcell, rsrp)
        (sg, (pci_freq, ta, ismcell, rsrp, isspinfo))
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
        val mRsrp = v2._2./:(0) {(x, y) => x + y._4.toInt} / sum
        // 如果有主小区存在 取主小区标识 (flag, ta, ismcell)
        val ismcell = v2._2.sortBy(_._3).reverse.head._3
        val isluce = v2._2.head._5
        val ta = v2._2.head._2
        (v2._1, ta, ismcell, mRsrp.toString, isluce)
      }.toArray
      (v1._1, fingerlineArr)
    }
    lcSpMap.size match{
      case 1 => {
        // 只存扫频数据 数据直接存
        var fArr = ArrayBuffer[Array[String]]()
        if(lcSpMap.head._1 == isspinfo){
          val value = lcSpMap.head._2.foreach{
            case (eNB, ta, ismcell, rsrp, isluce) => {
              var nrsrp = rsrp.toDouble.toInt
              if(lcSpMap.head._1 == isspinfo)
                nrsrp -= 8
              if(rejectByRssi(nrsrp.toString)){
                fArr += Array[String](eNB, ta, ismcell, nrsrp.toString)
              }
            }
          }
          if(fArr.length != 0){
            // 优先保留主服务小区的, 再保留临区的
            // Array(标识(bcch|bsic), ta, ismell, rxlevsub, sum)
            val fingerInfo = fArr.filter(_(2) == "1")
            val neiInfo = fArr.filter(_(2) == "0")
            if (fingerInfo.length >= finger_line_max_num) {
              val fstr = fingerInfo.sortBy(_(3).toInt).reverse.slice(0, finger_line_max_num).map(_.mkString(",")).mkString("$")
              fmap.put(key, fstr)
            } else {
              fingerInfo ++= neiInfo
              val fstr = fingerInfo.sortBy(_(3).toInt).reverse.slice(0, finger_line_max_num).map(_.mkString(",")).mkString("$")
              fmap.put(key, fstr)
            }
          }
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
            case (lceNB, lcta, lcismcell, lcrsrp, lcisluce) =>{
              if(rejectByRssi(lcrsrp)){
                fArr += Array[String](lceNB, lcta, lcismcell, lcrsrp.toDouble.toInt.toString)
              }
              spArr.foreach{
                case (speNB, spta, spismcell, sprsrp, spisluce) =>{
                  if(rejectByRssi(lcrsrp)){
                    if(lceNB == speNB){
                      vxiuzheng += abs(lcrsrp.toInt - sprsrp.toInt)
                      num += 1
                      spExitArr += speNB
                    }
                  }else{
                    if(lceNB == speNB){
                      vxiuzheng += abs(lcrsrp.toInt - sprsrp.toInt)
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
            case (speNB, spta, spismcell, sprsrp, spisluce) => {
              val frsrp = sprsrp.toInt - mxiuzheng
              if(rejectByRssi(frsrp.toString)){
                fArr += Array[String](speNB, spta, spismcell, frsrp.toInt.toString)
              }
            }
          }
          if(fArr.length != 0){
            // 优先保留主服务小区的, 再保留临区的
            // Array(标识(bcch|bsic), ta, ismell, rxlevsub, sum)
            val fingerInfo = fArr.filter(_(2) == "1")
            val neiInfo = fArr.filter(_(2) == "0")
            if (fingerInfo.length >= finger_line_max_num) {
              val fstr = fingerInfo.sortBy(_(3).toDouble.toInt).reverse.slice(0, finger_line_max_num).map(_.mkString(",")).mkString("$")
              fmap.put(key, fstr)
            } else {
              fingerInfo ++= neiInfo
              val fstr = fingerInfo.sortBy(_(3).toDouble.toInt).reverse.slice(0, finger_line_max_num).map(_.mkString(",")).mkString("$")
              fmap.put(key, fstr)
            }
          }
        }
      }
    }
  }

  def luceReduce(sg: String, Iter: Iterable[Array[String]], fmap: Map[String, String]): Unit = {
    if(sg != "") {
      // 安标识将相同纹线信息合并平均
      var lineArr2 = ArrayBuffer[ArrayBuffer[String]]()
      Iter.toList.groupBy(_(0)).foreach(x => {
        var lineArr1 = ArrayBuffer[String]()
        val sum = x._2.size
        val mRsrp = x._2./:(0) { (x, y) => x + y(3).toInt } / sum
        // 如果有主小区存在 取主小区标识 (flag, ta, ismcell)
        lineArr1 ++= x._2.sortBy(_(2)).reverse.head.slice(0, 3)
        lineArr1 += mRsrp.toString
        lineArr1 += sum.toString
        lineArr2 += lineArr1
      })

      // 优先保留主服务小区的, 再保留临区的
      // Array(标识(bcch|bsic), ta, ismell, rxlevsub, sum)
      val fingerInfo = lineArr2.filter(_(2) == "1")
      val neiInfo = lineArr2.filter(_(2) == "0")
      if (fingerInfo.length >= finger_line_max_num) {
        val fstr = fingerInfo.sortBy(_(3).toInt).reverse.slice(0, finger_line_max_num).map(_.mkString(",")).mkString("$")
      } else {
        fingerInfo ++= neiInfo
        val fstr = fingerInfo.sortBy(_(3).toInt).reverse.slice(0, finger_line_max_num).map(_.mkString(",")).mkString("$")
        fmap.put(sg, fstr)
      }
    }
  }


  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: <luce-file> <saopin-file>")
      System.exit(1)
    }

    RedisUtils.delTable(fingerlib_name)
//    val neiinfo = RedisUtils.getResultMap(neiLib_name)

    val conf = new SparkConf().setAppName("LcSpFingerLib1")
    val sc = new SparkContext(conf)

//    val nei = sc.broadcast(neiinfo)

    val luceRDD = sc.textFile(args(0)).map{x => luceMap(x)}.filter(_._1 != "")
    val spRDD = sc.textFile(args(1)).map{x => spMap(x)}.filter(_._1 != "")

    luceRDD.union(spRDD).groupByKey().foreachPartition{Iter =>
      val fmap = Map[String, String]()
      Iter.foreach(x => zReduce(x._1, x._2, fmap))
      tRedisPutMap.putMap2Redis(fingerlib_name, fmap.toMap)
    }

  }
}
