package cn.com.gis.shcs

/**
 * Created by wangxy on 16-1-19.
 */

import com.utils.ConfigUtils

import scala.collection.mutable.ArrayBuffer
import scala.math._

object fingergis0119 {

  val propFile = "/config/shanghai.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val finger_line_max_num = prop.getOrElse("FINGER_LINE_MAX_NUM", "12").toInt
  val data_per_grid = prop.getOrElse("DATA_PER_GIRD", "6").toInt
  val rsrp_down_border = prop.getOrElse("RSRP_DOWN_BORDER", "-100").toInt
  val grip_size = prop.getOrElse("GRID_SIZE", "25").toInt
  val rssi_uplimit = prop.getOrElse("RSSI_UPLIMIT", "-40").toInt
  val rssi_downlimit = prop.getOrElse("RSSI_DOWNLIMIT", "-100").toInt
  val bseq_index = prop.getOrElse("SEQ_INDEX", "5").toInt
  val bdiff_value = prop.getOrElse("DIFF_VALUE", "12").toInt
  //  val bmaxdiff_value = prop.getOrElse("MAXDIFF_VALUE", "97").toInt
  val isfilter_by_mcell = prop.getOrElse("ISFILTER_BY_MCELL", "1")
  val filterByDistance_percent = prop.getOrElse("FILTERBYDISTANCE_PERCENT", "0").toFloat
  //1:方差 2:绝对平均差 3:相关系数
  val calculate_choice = prop.getOrElse("CALCULATE_CHOICE", "1").toInt
  val samefactor_limit = prop.getOrElse("SAMEFACTOR_LIMIT", "1.0").toFloat
  val variance_limit = prop.getOrElse("VARIANCE_LIMIT", "99999999").toInt
  val averdiff_limit = prop.getOrElse("AVERDIFF_LIMIT", "97").toInt
  val similar_percent = prop.getOrElse("SIMILAR_PERCENT", "0.0").toFloat
  val istwice_compare = prop.getOrElse("ISTWICE_COMPARE", "0").toInt
  val twicedistance_limit = prop.getOrElse("TWICEDISTANCE_LIMIT", "100.0").toFloat
  val twicetime_limit = prop.getOrElse("TWICETIME_LIMIT", "3000.0").toLong
  val variance_offset = prop.getOrElse("VARIANCE_OFFSET", "100").toDouble
  val averdiff_offset = prop.getOrElse("AVERDIFF_OFFSET", "30").toDouble

  // 计算两经纬度距离
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

  // 墨卡托转经纬度
  def Mercator2lonlat(x: Int, y: Int): (Double, Double) = {
    val lon = x / 20037508.34 * 180
    var lat = y / 20037508.34 * 180
    lat = 180 / Pi * (2 * atan(exp(lat * Pi / 180)) - Pi / 2)
    (lon, lat)
  }


  // 电频值在范围内
  def rejectByRssi(info: ArrayBuffer[String]): Boolean = {
    info(3) != "" && info(3).toInt >= rssi_downlimit && info(3).toInt <= rssi_uplimit
  }

  // !!!!! 调这个函数时scandata是有序的,根据rssi由强到弱
  def getCandidateFinger(fingerprint: Array[(String, Array[Array[String]])], scandata: ArrayBuffer[ArrayBuffer[String]], tag: String):
  ArrayBuffer[(String, Array[Array[String]])] = {
    // 此处不区分指纹和数据各有几个主服务小区
    var finger = ArrayBuffer[(String, Array[Array[String]])]()
    var bfirst2 = true
    // 根据主服务小区匹配指纹库中主服务小区相同的记录 文档中的步骤2
    if(tag == "1"){
      scandata.foreach(x => {
        if(x(2) == "1"){
          fingerprint.foreach(y => {
            var bfirst1 = true
            for(i <- 0 to (y._2.length-1)){
              // 纹线信息 Array(标识(bcch|bsic), ta, ismell, rxlevsub, sum)
              if(y._2(i)(2)=="1" && y._2(i)(0)==x.head && bfirst1){
                finger += y
                bfirst1 = false
              }
            }
          })
        }
      })
    }

    //    finger.foreach(x => println("mserverfinger = "+x._1))

    // 文档中步骤3,4,5
    var finger1 = ArrayBuffer[(String, Array[Array[String]])]()

    // 两个门限值
    var seq_index = bseq_index
    var diff_value = bdiff_value

    scandata.foreach(x =>{
      if(finger1.size == 0) {
        fingerprint.foreach(y => {
          var bfirst2 = true
          var index = 1
          y._2.foreach(z => {
            if (bfirst2 && z(3) != "" && x(3) != "" && z(0) == x.head && index <= seq_index && abs(z(3).toFloat - x(3).toFloat) < diff_value) {
              finger1 += y
              bfirst2 = false
            }
            index += 1
          })
        })
      }
    })
    if(finger1.size == 0){
      seq_index = 12
      diff_value = 20
      scandata.foreach(x => {
        if (finger1.size == 0) {
          fingerprint.foreach(y => {
            var bfirst2 = true
            var index = 1
            y._2.foreach(z => {
              if (bfirst2 && z(3) != "" && x(3) != "" && z(0) == x.head && index <= seq_index && abs(z(3).toFloat - x(3).toFloat) < diff_value) {
                finger1 += y
                bfirst2 = false
              }
              index += 1
            })
          })
        }
      })
    }

    val mfinger = finger.toMap
    val finalfinger = ArrayBuffer[(String, Array[Array[String]])]()
    finger1.foreach{x =>
      if(mfinger.contains(x._1))
        finalfinger += x
    }

    if(finalfinger.size == 0)
      finger1
    else
      finalfinger

  }

  def getCorePoint(finger: ArrayBuffer[(String, Array[Array[String]])]): (Float, Float) = {
    var px = 0f
    var py = 0f
    finger.foreach(x => {
      val pxy = x._1.split("\\|", -1)
      px += pxy(0).toFloat
      py += pxy(1).toFloat
    })
    px /= finger.size
    py /= finger.size
    (px, py)
  }

  def getDistance(p1: (Float, Float), p2: (Float, Float)): Double = {
    sqrt(pow(p1._1-p2._1,2)+pow(p1._2-p2._2,2))
  }

  def filterByDistance(finger: ArrayBuffer[(String, Array[Array[String]])], point: (Float, Float)): ArrayBuffer[(String, Array[Array[String]], Double)] = {
    val fingerD = finger.map(x => {
      val pxy = x._1.split("\\|", -1)
      val d = getDistance((pxy(0).toFloat,pxy(1).toFloat), point)
      (x._1, x._2, d)
    })
    val num = floor(finger.size * filterByDistance_percent).toInt
    fingerD.sortBy(_._3).reverse.slice(0, finger.size-num)
  }

  def getCommonByFlag(finger: Array[Array[String]], scandata: ArrayBuffer[ArrayBuffer[String]]): (ArrayBuffer[ArrayBuffer[String]], ArrayBuffer[Array[String]]) = {
    val cfinger = ArrayBuffer[Array[String]]()
    val cdata = ArrayBuffer[ArrayBuffer[String]]()
    scandata.foreach(x => {
      finger.foreach(y => {
        if(x.head==y(0)){
          cdata += x
          cfinger += y
        }
      })
    })
    (cdata, cfinger)
  }

  // 提取数据指纹和库指纹中各对应纹线的rsrp
  def listToArray(finger: ArrayBuffer[Array[String]], scandata: ArrayBuffer[ArrayBuffer[String]]): (Array[Double], Array[Double]) = {
    val dfinger = ArrayBuffer[Double]()
    val ddata = ArrayBuffer[Double]()
    for(i <- 0 to (finger.length-1)){
      dfinger += finger(i)(3).toDouble
      ddata += scandata(i)(3).toDouble
    }
    (ddata.toArray, dfinger.toArray)
  }

  // 计算方差
  def getVariance(inputData: Array[Double]): Double = {
    val average = inputData./:(0.0)(_+_) / inputData.length
    var result = 0.0
    inputData.foreach{x =>
      result += pow(x-average, 2)
    }
    // 原来就是长度减1
    val res = result / (inputData.length - 1)
    res
  }

  // 计算相似系数
  def getCorrcoef(finger: Array[Double], scandata: Array[Double]): Double ={
    val averageT = scandata./:(0.0)(_+_) / scandata.length
    val averageL = finger./:(0.0)(_+_) / finger.length
    val cov = ArrayBuffer[Double]()
    for(i <- 0 to (scandata.length - 1)){
      cov += (scandata(i) - averageT) * (finger(i) -averageL)
    }
    var deviation = sqrt(getVariance(scandata)) * sqrt(getVariance(finger))
    if(deviation < 0.0000001)
      deviation = 0.0001
    val res = cov./:(0.0)(_+_) / (cov.length-1) / deviation
    res
  }

  def getDifByRssi(finger: Array[Double], scandata: Array[Double]): Array[Double] = {
    var dif = ArrayBuffer[Double]()
    for(i <- 0 to (finger.length-1)){
      dif += scandata(i) - finger(i)
    }
    dif.toArray
  }

  //计算 相同系数 方差 平均绝对差 相似系数
  // 返回值 Array[(栅格, Array[纹线], 距离中心点距离, 相同系数, 方差or平均绝对差or 相似系数)]
  def CalculateVarDiffSim(finger: ArrayBuffer[(String, Array[Array[String]], Double)], scandata: ArrayBuffer[ArrayBuffer[String]], flag: Int): ArrayBuffer[(String, Array[Array[String]], Double, Double, Double, Double)] = {
    finger.map(x => {
      val (cdata, cfinger) = getCommonByFlag(x._2, scandata)
      val sameFactor = cdata.size * 1.0 / min(x._2.length, scandata.size)
      val (ddata, dfinger) = listToArray(cfinger, cdata)
      val nSimilar = getCorrcoef(dfinger, ddata)
      var res = -1.0
      flag match {
        case 1 => {
          // 平均绝对差
          res = getDifByRssi(dfinger, ddata)./:(0.0){_ + abs(_)} / dfinger.length
          res /= sameFactor
          if(sameFactor < samefactor_limit)
            res += averdiff_offset
        }
        case 2 => {
          // 方差
          if(dfinger.length > 1)
            res = getVariance(getDifByRssi(ddata, dfinger))
          else
            res = abs(getDifByRssi(ddata, dfinger)(0))
          res /= sameFactor
          if(sameFactor < samefactor_limit)
            res += variance_offset
        }
        case _ => None
      }

      //      println("id="+x._1+" samefactor="+sameFactor+"  nSimilar="+nSimilar+"  res="+res)

      (x._1, x._2, x._3, sameFactor, res, nSimilar)
    })
  }

  def location(data: ArrayBuffer[ArrayBuffer[String]], fingerInfo: Array[(String, Array[Array[String]])]): (String, String) = {
      // mr数据处理
    var fxy = ("-1", "-1")
    val scandata = data
    val scandata1 = scandata.filter(rejectByRssi).sortBy(_(3).toInt).reverse.slice(0, 7)
    if (scandata1.length > 2) {
      // 指纹数据处理 !!!!scandata是有序的,根据rssi由强到弱
      val finger = getCandidateFinger(fingerInfo, scandata1, isfilter_by_mcell)
      if (finger.size != 0 && scandata1.size != 0) {
        //        println("finger1=" + finger.map(x => x._2.map(_.mkString(",")).mkString("^")).mkString("\n"))
        val pxy = getCorePoint(finger)
        val afinger = filterByDistance(finger, pxy)
        //        println("finger2=" + afinger.map(x => x._2.map(_.mkString(",")).mkString("^")).mkString("\n"))
        if (afinger.length != 0) {
          // 开始计算方差 绝对差 相似系数
          val tfinger = CalculateVarDiffSim(afinger, scandata1, calculate_choice).sortBy(_._6).reverse
          val ffinger = tfinger.slice(0, (tfinger.length * (1.0 - similar_percent)).toInt)

          calculate_choice match {
            case _ => {
              // 方差和平均绝对差越小越好
              val sg = ffinger.sortBy(_._5).head._1.split("\\|")
              fxy = (sg(0), sg(1))
            }
          }
        }
      }
    }
    fxy
  }

  def twiceCompare(data: Array[(Long, (String, String), Array[String])]): Array[(Long, (String, String), Array[String])] = {
    if(istwice_compare == 1){
      var osg = ("-1", "-1")
      var lasttime = 0L
      data.sortBy(_._1).map{
        case (time, (x, y), otherInfo) =>{
          val nowtime = time
          if (0 == lasttime) {
            lasttime = nowtime
            osg = (x, y)
          } else {
            if (abs(nowtime - lasttime) <= twicetime_limit) {
              lasttime = nowtime
              val nlonlat = Mercator2lonlat(x.toInt * grip_size, y.toInt * grip_size)
              val olonlat = Mercator2lonlat(osg._1.toInt * grip_size, osg._2.toInt * grip_size)
              val d = calc_distance(olonlat._1, olonlat._2, nlonlat._1, nlonlat._2)
              if (d > twicedistance_limit) {
                val (fx, fy) = (rint((x.toLong + osg._1.toLong) / 2).toLong.toString, rint((y.toLong + osg._2.toLong) / 2).toLong.toString)
                osg = (fx, fy)
              } else {
                osg = (x,y)
              }
            } else{
              lasttime = nowtime
              osg = (x, y)
            }
          }
          (time, osg, otherInfo)
        }
      }
    }else{
      data
    }
  }
}
