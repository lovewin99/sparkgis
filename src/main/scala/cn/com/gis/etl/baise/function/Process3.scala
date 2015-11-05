package cn.com.gis.etl.baise.function

/**
 * Created by wangxy on 15-11-5.
 */

import com.utils.RedisUtils

import scala.math._
import cn.com.gis.tools._

import scala.collection.mutable.{ListBuffer, Map}

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

object Process3 {
  var m_dbWeigBase = 1.0        // 权值由1调整为0.6 0.8 1.2 1.4 1.6，测试结果以确定合理权值
  var disper_longitude_ = 0.0
  var disper_latitude_ = 0.0

  var CellInfo = Map[Int, StaticCellInfo2]()
  var NeiInfo = Map[String, Int]()              //["cellid,pci_freq", cellid(临区的)]

  var x = 11626986.857844816
  var y = 2614746.4238366615

  val aM = 6378245.0
  val ee = 0.00669342162296594323
  val x_pi = Pi * 3000.0 / 180.0

  val mr_length = 63
  val ta_index = 4
  val aoa_index = 6
  val serfreq_index = 0
  val serrsrp_index = 2
  val cellid_index = 28
  val ues1apid_index = 29
  val code_index = 30
  val groupid_index = 31
  val btime_index = 32

  val nei_maxnum = 6
  val nei_length = 5
  val neibegin_index = 33
  // 临区信息相对位置
  val neifreq_index = 0
  val neipci_index = 1
  val neirsrp_index = 2

  def Setup(): (Map[Int, StaticCellInfo2], Map[String, Int]) ={

    val tmpCellInfo = Map[Int, StaticCellInfo2]()
    val tmpNeiInfo = Map[String, Int]()              //["cellid,pci_freq", cellid(临区的)]
    // 初始化计算位置需要的参数
    //    Process.init

    //读取基础表
    val cellinfo = RedisUtils.getResultMap("baseinfo1")
    cellinfo.foreach(e => {
      val c_info = new StaticCellInfo2
      val strArr = e._2.split("\t")
      if (strArr.length == 11){
        c_info.cellid_ = e._1.toInt
        c_info.longitude_ = strArr(2).toDouble
        c_info.latitude_ = strArr(3).toDouble
        c_info.freq_ = strArr(7).toInt
        c_info.cell_pci_ = strArr(8).toInt
        c_info.in_door_ = strArr(9).toInt
        c_info.azimuth_ = strArr(10).toInt

        tmpCellInfo.put(e._1.toInt, c_info)
      }
    })

    //读取临区表
    val neiinfo = RedisUtils.getResultMap("neiinfo1")
    neiinfo.foreach(e => {
      val key = e._1
      val value = e._2.toInt
      tmpNeiInfo.put(key, value)
    })
    (tmpCellInfo, tmpNeiInfo)
  }

  def init(): Unit ={
    val center_latitude = 23.91
    val center_longgitude = 106.62

    disper_longitude_ = calc_distance(center_longgitude - 1, center_latitude, center_longgitude + 1, center_latitude) / 2
    disper_latitude_ = calc_distance(center_longgitude, center_latitude - 1, center_longgitude, center_latitude + 1) / 2
  }


  def mapProcess(in: String) : (String, String)={
    val strArr = in.split("\\|", -1)

    if(strArr.length == mr_length){
      //MmeUeS1apId,MmeCode,MmeGroupId 三个字段标识用户
      val userId = strArr(ues1apid_index) + "," + strArr(code_index) + "," + strArr(groupid_index)

      val time = strArr(btime_index).replaceAll("[\\-: ]", "")
      (userId, in + "|" +time)
    } else {
      ("-1", "-1")
    }
  }


  def reduceProcess(key : String, Iter : Iterable[String], cellmap : Map[Int, StaticCellInfo2], neimap : Map[String, Int]): String = {

    if(key.equals("-1")){
      "-1" + "|" + "-1" + "|" + "-1" + "|" + "-1"
    }
    else {
      init

      CellInfo = cellmap
      NeiInfo = neimap
      val vlist = Iter.toList.map(_.split("\\|")).sortBy(_(33))
      var usr = new User2
      val a = vlist.map{e =>
        val xdr = new XDR_UE_MR_S2

        xdr.cell_id_ = if (e(cellid_index) != "") e(cellid_index).toInt else -1
        xdr.ta_ = if (e(ta_index) != "") e(ta_index).toInt else -1
        xdr.aoa_ = if (e(aoa_index) != "") e(aoa_index).toInt else -1
        xdr.serving_freq_ = if (e(serfreq_index) != "") e(serfreq_index).toInt else -1
        xdr.serving_rsrp_ = if (e(serrsrp_index) != "") e(serrsrp_index).toInt else -1
        xdr.time_ = e(mr_length).toLong

        var mrneiinfo = ListBuffer[(Long, Int)]()
        for(i <- 0 to (nei_maxnum -1)){
          val cur = neibegin_index + nei_length * i
          if(e(cur+neifreq_index) != "" && e(cur+neipci_index) != "" && e(cur+neirsrp_index) != ""){
            val neipci_freq = e(cur+neipci_index).toLong << 16 | e(cur+neifreq_index).toLong
            val neirsrp = e(cur+neirsrp_index).toInt
            mrneiinfo += ((neipci_freq, neirsrp))
          }
        }

//        println("mrneiinfo = " + mrneiinfo)


        // 计算经纬度
        val res = locate(xdr, usr, mrneiinfo)

        usr = res._3
        val time: Long = xdr.time_ / 100000000
        val rsrp = xdr.serving_rsrp_

        if(res._1 == -1 || res._1 == -1){
          // 如果没有算出经纬度(广西)
          time.toString + "|" + "-1" + "|" + "-1" + "|" + rsrp + "|" + res._4
        } else{
          val mcoo = transform2Mars(res._1, res._2)
          val coo = lonLat2Mercator(mcoo._1, mcoo._2)
          val sgX = if ((coo._1 - x) % 100 != 0) ((coo._1 - x) / 100 + 1).toInt else ((coo._1 - x) / 100).toInt
          val sgY = if ((coo._2 - y) % 100 != 0) ((coo._2 - y) / 100 + 1).toInt else ((coo._2 - y) / 100).toInt
          //          val time: Long = xdr.time_ / 100000000
          val rsrp = xdr.serving_rsrp_
          time.toString + "|" + sgX.toString + "|" + sgY.toString + "|" + rsrp.toString + "|" + res._4
        }
      }

      a.mkString("\n")
    }
  }

  def lastword(n : Double) : String = {
    val a = (n * 1000).toInt
    var str = ""
    if(a / 100000 != 0)
      str = a.toString
    else if(a / 10000 != 0)
      str = "0" +a.toString
    else if(a / 1000 != 0)
      str = "00" +a.toString
    else if(a / 100 != 0)
      str = "000" +a.toString
    else if(a / 10 != 0)
      str = "0000" +a.toString
    else
      str = "00000" +a.toString
    str
  }

  def baoliu(n : Double) : String = {
    val a = n.toInt
    val b = ((n - a) * 100000).toInt
    var str = ""
    if(b / 10000 != 0)
      str = b.toString
    else if(b / 1000 != 0)
      str = "0" +b.toString
    else if(b / 100 != 0)
      str = "00" +b.toString
    else if(b / 10 != 0)
      str = "000" +b.toString
    else
      str = "0000" +b.toString

    a + "." + str
  }

  // 地球坐标转火星坐标
  //*******************************************************/
  def outOfChina(lon : Double, lat : Double) : Boolean ={
    if(lon < 72.004 || lon > 137.8347 || lat < 0.8293 || lat > 55.8271){
      true
    }else{
      false
    }
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

  def transform2Mars(wgLon : Double, wgLat : Double) : (Double, Double)={
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

  def locate(sdata : XDR_UE_MR_S2, udata : User2, mrneiinfo: ListBuffer[(Long, Int)]) : (Double, Double, User2, Int) ={
    var aoa = sdata.aoa_
    aoa match{
      //如果数据没有aoa信息
      case -1 => {
        udata.aoa_ match{
          //查看用户是否有aoa信息
          case -1 => None
          case _ => {
            //是否5秒内 是则使用udata的aoa
            val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
            val datas = sdf.parse(sdata.time_.toString).getTime
            val datau = sdf.parse(udata.time.toString).getTime
            if (abs(datas - datau) < 5000) {
              aoa = udata.aoa_
            }
          }
        }
      }
      //如果数据有aoa信息 更新用户信息
      case _ =>{
        udata.aoa_ = sdata.aoa_
        udata.cell_id = sdata.cell_id_
        udata.time = sdata.time_
      }
    }

    var ta = sdata.ta_
    ta match{
      //如果数据没有ta信息
      case -1 => {
        udata.ta_ match{
          //查看用户是否有ta信息
          case -1 => None
          case _ => {
            //是否5秒内 是则使用udata的ta
            val sdf = new SimpleDateFormat("yyyyMMddmmss")
            val datas = sdf.parse(sdata.time_.toString).getTime
            val datau = sdf.parse(udata.time.toString).getTime
            if (abs(datas - datau) < 5000) {
              ta = udata.ta_
            }
          }
        }
      }
      //如果数据有ta信息 更新用户信息
      case _ =>{
        udata.ta_ = sdata.ta_
        udata.cell_id = sdata.cell_id_
        udata.time = sdata.time_
      }
    }

    // 通过aoa, ta 计算经纬度
    val res = locate_by_ta_aoa(aoa, ta, sdata.cell_id_)
    res._1 match{
      case -1 => {
        // 如果第一种算法计算失败　则使用第二种算法
        val res1 = locate_by_nei_level(sdata, mrneiinfo)
        (res1._1, res1._2, udata, res1._3)
      }
      case _ => (res._1, res._2, udata, res._3)
    }
  }

  def locate_by_ta_aoa(aoa : Int, ta : Int, cell_id : Int) : (Double, Double, Int) ={
    val angle = aoa / 2       //aoa ta 没有ntohs
    val cellinfo_ = CellInfo.getOrElse(cell_id, new StaticCellInfo2)

    if (aoa != -1 && angle < 360 && ta != -1 && ta < 2048 && cellinfo_.cellid_ != -1){
      val radius = distance(ta)
      val radian = (360 - angle) * Pi / 180
      val x = cos(radian) * radius
      val y = sin(radian) * radius
      val longitude_ : Double = cellinfo_.longitude_ - y / disper_longitude_
      val latitude_ : Double = cellinfo_.latitude_ - x / disper_latitude_
      //      println("radius="+radius+"  radian="+radian+" x="+x+"  y="+y+" longitude_=" + longitude_ +"  latitude_="+latitude_ +" disper_longitude_="+disper_longitude_ +
      //        "  disper_latitude_=" + disper_latitude_ + " cellinfo_.longitude_=" +cellinfo_.longitude_ + " cellinfo_.latitude_="+cellinfo_.latitude_)
      (longitude_, latitude_, 1)
    } else{
      (-1, -1, -1)
    }
  }

  def locate_by_nei_level(sdata : XDR_UE_MR_S2, mrneiinfo: ListBuffer[(Long, Int)]) : (Double, Double, Int) ={
    var longitude_ : Double = -1
    var latitude_ : Double = -1
    val cellId = sdata.cell_id_
    val main_cell = CellInfo.getOrElse(cellId, new StaticCellInfo2)
    main_cell.cellid_ match{
      // 如果在字典表里没有找到 返回(-1, -1)
      case -1 => {
        (longitude_, latitude_, -2)
      }
      case _ => {
        mrneiinfo.length match{
          // 如果临区信息不存在
          case 0 => {
            var radius = 0.0
            val ta = sdata.ta_
            var tmp = 1
            if (ta < 2048 && ta != -1) {
              radius = distance(ta) / 100000
            } else if (-1 != sdata.serving_rsrp_){
              radius = GetDistanceByRsrp(sdata.serving_rsrp_, 1) / 100000
            } else {
              radius = (random * 1000).toInt * 0.000001
            }
            val rad = random
            val radian = (360 - main_cell.azimuth_) * Pi / 180 + (random - 0.5 ) * (1.0 + radius * 10.0)
            val nPosX = cos(radian) * radius
            val nPosY = sin(radian) * radius

            val rad1 = random - 0.1
            if (rad1 <= 0){
              // 10% 落在小区背面
              longitude_ = main_cell.longitude_ - nPosX
              latitude_ = main_cell.latitude_ - nPosY
            } else {
              longitude_ = main_cell.longitude_ + nPosX
              latitude_ = main_cell.latitude_ + nPosY
            }
            (longitude_, latitude_, 2)
          }
          case _ =>{
            //有临区信息
            main_cell.in_door_ match{
              case 0 => {
                // 室内
                val ll1 = GetInsideMRLonLat(main_cell)
                (ll1._1, ll1._2, 3)
              }
              case _ => {
                //　室外
                var lnei = ListBuffer[(StaticCellInfo2, Int)]()
                mrneiinfo.foreach(x => {
                  val neiinfo_cellid1 = NeiInfo.getOrElse(cellId+","+x._1.toString, -1)
                  if(neiinfo_cellid1 != -1){
                    val neiinfo1 = CellInfo.getOrElse(neiinfo_cellid1, new StaticCellInfo2)
                    if(x._2 >= 0 && x._2 <= 97){
                      lnei += ((neiinfo1, x._2))
                    }
                  }
                })
                if(lnei.length >= 2){
                  if(sdata.serving_rsrp_ > 97 || sdata.serving_rsrp_ < 0){
                    (-1, -1, -14)
                  }else{
                    val ll2 = GetLocInfo(sdata, main_cell, lnei)
                    (ll2._1, ll2._2, 5)
                  }
                }else{
                  (-1, -1, -15)
                }

//                val pci_freq = (sdata.nei_cell_pci_ << 16) | sdata.nei_freq_
//                val neiinfo_key = cellId.toString + "," + pci_freq.toString
//                val neiinfo_cellid = NeiInfo.getOrElse(neiinfo_key, -1)
//                //                println("neiinfo_key="+neiinfo_key + " nei_cell_pci=" + sdata.nei_cell_pci_ + "  nei_freq_=" + sdata.nei_freq_)
//                neiinfo_cellid match{
//                  case -1 => {(longitude_, latitude_, -3)}    //临区信息在redis表中未找到
//                  case _ => {
//                    val neiinfo = CellInfo.getOrElse(neiinfo_cellid, new StaticCellInfo2)
//                    neiinfo.cellid_ match{
//                      case -1 => (longitude_, latitude_, -4)
//                      case _ => if (sdata.nei_rsrp_ == -1 || sdata.serving_rsrp_ > 97) (-1, -1, -14)
//                      else {
//                        val ll2 = GetLocInfo(sdata, main_cell, neiinfo)
//                        (ll2._1, ll2._2, 5)
//                      }
//                    }
//                  }
//                }
              }
            }
          }
        }
      }
    }
  }


  def GetLocInfo(sdata: XDR_UE_MR_S2, main_cell : StaticCellInfo2, nei_cell : ListBuffer[(StaticCellInfo2, Int)]) : (Double, Double) = {
//  def GetLocInfo(sdata: XDR_UE_MR_S2, main_cell : StaticCellInfo2, nei_cell : StaticCellInfo2) : (Double, Double) = {
    var longi = main_cell.longitude_
    var lati = main_cell.latitude_
    var x_ave = longi
    var y_ave = lati
    var x_sqrt = 0.0
    var y_sqrt = 0.0

    nei_cell.foreach(x => {
      x_ave += x._1.longitude_
      y_ave += x._1.latitude_
    })

    x_ave /= (nei_cell.length + 1)
    y_ave /= (nei_cell.length + 1)

    x_sqrt = pow(longi - x_ave, 2)
    y_sqrt = pow(lati - y_ave, 2)

    nei_cell.foreach(x => {
      x_sqrt += pow(x._1.longitude_ - x_ave, 2)
      y_sqrt += pow(x._1.latitude_ - y_ave, 2)
    })

    x_sqrt /= nei_cell.length
    y_sqrt /= nei_cell.length
    x_sqrt = sqrt(x_sqrt)
    y_sqrt = sqrt(y_sqrt)

    val valid_point = ListBuffer[Int]()
    var index = 0
    nei_cell.foreach(x => {
      if(abs(x._1.longitude_ - main_cell.longitude_) < 0.00001 && abs(x._1.latitude_ - main_cell.latitude_) < 0.00001){
        // 同站
      }
      else if(abs(x._1.longitude_ - longi) >= 2.5*x_sqrt ||
                abs(x._1.latitude_ - lati) >= 2.5*y_sqrt){
        // 删除非法点
      }
      else{
        valid_point += index
      }
      index += 1
    })

    var last_neib_rsrp = 0
    if(valid_point.length > 0){
      last_neib_rsrp = nei_cell(valid_point(valid_point.length - 1))._2
    }else {
      last_neib_rsrp = sdata.serving_rsrp_
    }

    val weig_base = m_dbWeigBase
    if(sdata.serving_rsrp_ < last_neib_rsrp){
      last_neib_rsrp = sdata.serving_rsrp_
    }

    var weig_cell = (sdata.serving_rsrp_ - last_neib_rsrp)*1.0/10 + weig_base
    var weig_sum = 0.0
    if(weig_cell >= weig_base){
      weig_sum += weig_cell
      longi *= weig_cell
      lati *= weig_cell
    }

    valid_point.foreach(x => {
      weig_cell = (nei_cell(x)._2 - last_neib_rsrp) * 1.0 / 10 + weig_base
      if(weig_cell >= weig_base){
        longi += (nei_cell(x)._1.longitude_ * weig_cell)
        lati += (nei_cell(x)._1.latitude_ * weig_cell)
        weig_sum += weig_cell
      }
    })

    longi /= weig_sum
    lati /= weig_sum

    // 去掉定位直线
    if (main_cell.longitude_ != -1 && main_cell.latitude_ != -1 && valid_point.length < 3){
      //      val rad = if((random - 0.5) > 0)  1 else -1
      val dbDistance = sqrt((longi - main_cell.longitude_) * (longi - main_cell.longitude_) +
        (lati - main_cell.latitude_) * (lati - main_cell.latitude_))
      longi = longi -dbDistance * 0.996 * (if((random - 0.5) > 0)  1 else -1)
      lati = lati + dbDistance * 0.0872 * (if((random - 0.5) > 0)  1 else -1)
    }

//    if(abs(longi - main_cell.longitude_)>1 || abs(lati - main_cell.latitude_)>1){
//      println("main_cell.longitude_="+main_cell.longitude_ +" main_cell.longitude_="+main_cell.latitude_)
//      println("nei_cell="+nei_cell)
//      println("longi="+longi+" lati="+lati+"  lastrsrp="+last_neib_rsrp+"    weig_sum="+weig_sum)
//      println("sdata.serving_rsrp_="+sdata.serving_rsrp_)
//    }

    (longi, lati)
  }

  def GetInsideMRLonLat(main_cell : StaticCellInfo2): (Double, Double) = {
    // 室内计算
    var longitude_ : Double = -1
    var latitude_ : Double = -1

    val nAngle = (random * 1000).toInt % 360
    val nRadio = (random * 100).toInt % 50

    val dx = nRadio * cos(nAngle) / disper_longitude_
    val dy = nRadio * sin(nAngle) / disper_latitude_
    longitude_ = main_cell.longitude_ + dx
    latitude_ = main_cell.latitude_ + dy

    (longitude_, latitude_)
  }

  def GetDistanceByRsrp(rsrp : Int, n : Double) : Double ={
    val dbm = rsrp_to_dbm(rsrp)
    val dA1 = 43.0 - 0.605596 * 2 + 7 + dbm
    val res = pow(10, dA1 / 10 / n)
    res
  }

  def rsrp_to_dbm(rsrp : Int) : Double = {
    var tmp = rsrp
    if (rsrp > 97) {
      tmp = 97
    }
    val res = -140.0 + tmp
    res
  }

  def distance(ta : Int) : Double = {
    val ret = 4.89 * ta
    ret
  }

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

  def main(args : Array[String]): Unit ={
    if (args.length != 2) {
      System.err.println("Usage: <in-file> <out-file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("sparkgis Application")
    val sc = new SparkContext(conf)

    val textRDD = sc.textFile(args(0))

    val xmap = Setup
    val x = sc.broadcast(xmap._1)
    val y = sc.broadcast(xmap._2)
    println("!!!!!!!!!!!!!!!!!!!!!size=================>"+xmap._2.size)

    val result = textRDD.mapPartitions(Iter => {
      Iter.map(e => mapProcess(e))
    }).groupByKey().map(e => reduceProcess(e._1, e._2, x.value, y.value))


    result.saveAsTextFile(args(1))
  }
}
