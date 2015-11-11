package cn.com.gis.etl.shanghai

import com.utils.ConfigUtils

import scala.math._
import scala.collection.mutable.ArrayBuffer

/**
 * Created by wangxy on 15-11-11.
 */
object gsmgis1 {

  val propFile = "/config/shanghai.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val finger_line_max_num = prop.getOrElse("FINGER_LINE_MAX_NUM", "12").toInt
  val rsrp_down_border = prop.getOrElse("RSRP_DOWN_BORDER", "-100").toInt
  val grip_size = prop.getOrElse("GRID_SIZE", "25").toInt
  val rssi_uplimit = prop.getOrElse("RSSI_UPLIMIT", "-40").toInt
  val rssi_downlimit = prop.getOrElse("RSSI_DOWNLIMIT", "-100").toInt
  val bseq_index = prop.getOrElse("SEQ_INDEX", "6").toInt
  val bdiff_value = prop.getOrElse("DIFF_VALUE", "12").toInt
  val bmaxdiff_value = prop.getOrElse("MAXDIFF_VALUE", "97").toInt
  val isfilter_by_mcell = prop.getOrElse("ISFILTER_BY_MCELL", "1")
  val filterByDistance_percent = prop.getOrElse("FILTERBYDISTANCE_PERCENT", "0").toFloat

  // 确定有的
  val mr_length = 42
  val mr_time_index = 0
  val mr_lac_index = 3
  val mr_ci_index = 4
  val mr_mrxlev_index = 9
  val mr_ta_index = 10

  val mr_nei_maxnum = 6

  //临时信息
  val Finger_length = 11
  val Sampling_index = 0
  val time_index = 1
  val lon_index = 2
  val lat_index = 3
  val lac_index = 4
  val ci_index = 5
  val bcch_index = 6
  val bsic_index = 7
  val ta_index = 8
  val relevsub_index = 9
  val ismcell_index = 10

  // 最大临区个数
  val nei_maxnum = 12

  //经纬度转墨卡托
  def lonLat2Mercator(lon : Double, lat : Double) : (Double, Double) = {
    val x = lon * 20037508.342789 / 180
    var y = log(tan((90+lat)*Pi/360)) / (Pi / 180)
    y = y * 20037508.34789 / 180
    (x, y)
  }

  def outOfChina(lon : Double, lat : Double) : Boolean ={
    lon < 72.004 || lon > 137.8347 || lat < 0.8293 || lat > 55.8271
  }

  //((采样点,时间,栅格),(标识,ta,ismain,rxlevsub))
  def tmpInmapProcess(in: String): ((String, String, String), Array[String]) = {
    val strArr = in.split(",", -1)
    if(Finger_length == strArr.length){
      val lon = strArr(lon_index).toDouble
      val lat = strArr(lat_index).toDouble
      if(!outOfChina(lon, lat)){
        val sampling = strArr(Sampling_index)
        val time = strArr(time_index)
        val coo = lonLat2Mercator(lon, lat)
        val sg = coo._1 + "|" + coo._2
        val flag = strArr(bcch_index) + "|" + strArr(bsic_index)
        val ta = strArr(ta_index)
        val ismain = strArr(ismcell_index)
        val rxlev = strArr(relevsub_index)
        val value1 = Array[String](flag, ta, ismain, rxlev)
        ((sampling, time, sg), value1)
      }else {
        (("-1", "-1", "-1"), Array[String]())
      }
    }else {
      (("-1", "-1", "-1"), Array[String]())
    }
  }

  // (用户, (公共信息, 定位信息))
  // 公共信息: 时间,栅格  定位信息: 多个纹线   纹线: 标识,ta,ismain,rxlevsub
  def tmpreduceProcess(key: (String, String, String), Iter: Iterable[Array[String]]):
        (String, (Array[String], ArrayBuffer[ArrayBuffer[String]])) = {
    val time = key._2
    val sg = key._3

    var lineArr2 = ArrayBuffer[ArrayBuffer[String]]()
    Iter.toList.groupBy(_(0)).foreach(x => {
      var lineArr1 = ArrayBuffer[String]()
      val sum = x._2.size
      val mRsrp = x._2./:(0) { (x, y) => x + y(3).toInt } / sum
      lineArr1 ++= x._2.sortBy(_(2)).reverse.head.slice(0, 3)
      lineArr1 += mRsrp.toString
      lineArr2 += lineArr1
    })
    ("1", (Array[String](time,sg), lineArr2))
  }

  // 电频值在范围内
  def rejectByRssi(info: ArrayBuffer[String]): Boolean = {
    info(3).toInt < rssi_downlimit && info(3).toInt > rssi_uplimit
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
            for(i <- 1 to finger_line_max_num){
              var bfirst1 = true
              if(y._2(i)(2)=="1" && y._2(i)(0)==x.head && bfirst1){
                finger += y
                bfirst1 = false
              }
            }
          })
        }
      })
    }
    // 文档中步骤3,4,5
    if(finger.size == 0){
      // 两个门限值
      var seq_index = bseq_index
      var diff_value = bdiff_value
      while(seq_index <= finger_line_max_num && diff_value < 97 && finger.size == 0){
        scandata.foreach(x =>{
          if(finger.size == 0){
            fingerprint.foreach(y => {
              var bfirst2 = true
              var index = 1
              y._2.foreach(z => {
                if(bfirst2 && z(3) != "" && x(3) != "" && z(0)==x.head && index <= seq_index && abs(z(3).toInt-x(3).toInt)<diff_value){
                  finger += y
                  index += 1
                  bfirst2 = false
                }
              })
            })
          }
        })
        if(finger.size == 0){
          seq_index += 1
          diff_value += 2
        }
      }
    }
    finger
  }

  def getCorePoint(finger: ArrayBuffer[(String, Array[Array[String]])]): (Int, Int) = {
    var px = 0
    var py = 0
    finger.foreach(x => {
      val pxy = x._1.split("|", -1)
      px += pxy(0).toInt
      py += pxy(1).toInt
    })
    px /= finger.size
    py /= finger.size
    (px, py)
  }

  def getDistance(p1: (Int, Int), p2: (Int, Int)): Double = {
    sqrt(pow(p1._1-p2._1,2)+pow(p1._2-p2._2,2))
  }

  def filterByDistance(finger: ArrayBuffer[(String, Array[Array[String]])], point: (Int, Int)): ArrayBuffer[(String, Array[Array[String]], Double)] = {
    val fingerD = finger.map(x => {
      val pxy = x._1.split("|", -1)
      val d = getDistance((pxy(0).toInt,pxy(1).toInt), point)
      (x._1, x._2, d)
    })
    val num = floor(finger.size * filterByDistance_percent).toInt
    fingerD.sortBy(_._3).reverse.slice(0, finger.size-num)
  }

  def location(key: String, Iter: Iterable[(Array[String], ArrayBuffer[ArrayBuffer[String]])],
               fingerInfo: Array[(String, Array[Array[String]])]): String = {
    Iter.toList.sortBy(_._1(0)).map(x =>{
      // mr数据处理
      var scandata = x._2
      scandata = scandata.filter(rejectByRssi).sortBy(_(3)).reverse
      println("scandata=" + scandata.mkString(","))
      // 指纹数据处理 !!!!scandata是有序的,根据rssi由强到弱
      val finger = getCandidateFinger(fingerInfo, scandata, isfilter_by_mcell)
      val pxy = getCorePoint(finger)
      val afinger = filterByDistance(finger, pxy)

      if(finger.size != 0){
        "-1"
      }else {
        "-1"
      }
    }).mkString("\n")
  }
}
