package cn.com.gis.etl.location.function

/**
 * Created by wangxy on 16-1-5.
 */

import com.utils.{RedisUtils, ConfigUtils}

import scala.math._

import scala.collection.mutable.{ListBuffer, Map}

import java.text.SimpleDateFormat

import scala.collection.immutable.{Map=>imMap}

object gxFunction1 {
  val propFile = "/config/mrlocation.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val seprator = prop.getOrElse("MR.SEPRATOR", ",")
  val grid_size = prop.getOrElse("GRID_SIZE", "100").toInt

  val mr_length = prop.getOrElse("mr_length", "100").toInt
  val index_imsi = prop.getOrElse("index_imsi", "100").toInt
  val index_imei = prop.getOrElse("index_imei", "100").toInt
  val index_msisdn = prop.getOrElse("index_msisdn", "100").toInt
  val index_groupid = prop.getOrElse("index_groupid", "100").toInt
  val index_code = prop.getOrElse("index_code", "100").toInt
  val index_s1ap = prop.getOrElse("index_s1ap", "100").toInt
  val index_enbid = prop.getOrElse("index_enbid", "100").toInt
  val index_cellid = prop.getOrElse("index_cellid", "100").toInt
  val index_time = prop.getOrElse("index_time", "100").toInt
  val index_ta = prop.getOrElse("index_ta", "100").toInt
  val index_aoa = prop.getOrElse("index_aoa", "100").toInt
  val index_sfreq = prop.getOrElse("index_sfreq", "100").toInt
  val index_spci = prop.getOrElse("index_spci", "100").toInt
  val index_srsrp = prop.getOrElse("index_srsrp", "100").toInt
  val index_neinum = prop.getOrElse("index_neinum", "100").toInt

  // 单个临区信息长度
  val nei_length = prop.getOrElse("nei_length", "100").toInt
  // 临区信息相对位置
  val neipci_index = prop.getOrElse("neipci_index", "100").toInt
  val neifreq_index = prop.getOrElse("neifreq_index", "100").toInt
  val neirsrp_index = prop.getOrElse("neirsrp_index", "100").toInt

  //  工参字段顺序
  val gc_lon = prop.getOrElse("gc_lon", "100").toInt
  val gc_lat = prop.getOrElse("gc_lat", "100").toInt
  val gc_pci = prop.getOrElse("gc_pci", "100").toInt
  val gc_freq = prop.getOrElse("gc_freq", "100").toInt
  val gc_inout = prop.getOrElse("gc_inout", "100").toInt
  val gc_angle = prop.getOrElse("gc_angle", "100").toInt
  val gc_downangle = prop.getOrElse("gc_downangle", "100").toInt

  val baselibname = prop.getOrElse("BSBASE_LIB", "")
  val neilibname = prop.getOrElse("BSNEI_LIB", "")
  val sglibname = prop.getOrElse("SHANGE_LIB", "")

  //  栅格信息字段
  val sg_x        = prop.getOrElse("sg_x", "0").toInt
  val sg_y        = prop.getOrElse("sg_y", "0").toInt
  val sg_lon      = prop.getOrElse("sg_lon", "0").toInt
  val sg_lat      = prop.getOrElse("sg_lat", "0").toInt
  val sg_tac      = prop.getOrElse("sg_tac", "0").toInt
  val sg_eci      = prop.getOrElse("sg_eci", "0").toInt
  val sg_area     = prop.getOrElse("sg_area", "0").toInt
  val sg_traffic  = prop.getOrElse("sg_traffic", "0").toInt
  val sg_road     = prop.getOrElse("sg_road", "0").toInt
  val sg_roadcode = prop.getOrElse("sg_roadcode", "0").toInt
  val sg_roadname = prop.getOrElse("sg_roadname", "0").toInt

  var m_dbWeigBase = 1.0        // 权值由1调整为0.6 0.8 1.2 1.4 1.6，测试结果以确定合理权值
  var disper_longitude_ = 0.0
  var disper_latitude_ = 0.0

  var CellInfo = Map[String, StaticCellInfo2]()
  var NeiInfo = imMap[String, String]()              //["cellid,pci_freq", cellid(临区的)]

  //  var x = 11626986.857844816
  //  var y = 2614746.4238366615

  val aM = 6378245.0
  val ee = 0.00669342162296594323
  val x_pi = Pi * 3000.0 / 180.0

  //待处理数据
  class XDR_UE_MR_S2 extends Serializable{
    var cell_id_ = ""
    var ta_ : Int = -1
    var aoa_ : Int = -1
    var serving_freq_ : Int = -1
    var serving_rsrp_ : Int = -1
    var time_ : Long = -1

    // 以下为临区信息
    //  var nei_cell_pci_ : Int = -1
    //  var nei_freq_ : Long = -1L
    //  var nei_rsrp_ : Long = -1L
    //  var nei_rsrq_ : Int = -1
  }

  //临时保存的用户信息
  class User2 extends Serializable{
    var cell_id = ""
    var ta_ = -1
    var aoa_ = -1
    var time = -1L
  }

  class StaticCellInfo2 extends Serializable{
    var enb_height_ = 40.0        // 挂高(m)
    var crs_power_ = 7.2           //
    var ant_gain_ = 14.5           // db
    var in_door_  = "室内"                  // 室内小区

    var cellid_ = "-1"                  // enbid<<8 |	cell_in_enb
    var longitude_ = 0.0           // longitude
    var latitude_ = 0.0            //	latitude
    var cell_pci_ = -1L	              // cell_pci
    var freq_ = -1L                    // freq
    var azimuth_ = -1                  //方向角			azimuth
    var pci_freq = -1L

  }

  def Setup(): Map[String, StaticCellInfo2] ={

    val tmpCellInfo = Map[String, StaticCellInfo2]()
    //    val tmpNeiInfo = Map[String, Int]()              //["cellid,pci_freq", cellid(临区的)]
    // 初始化计算位置需要的参数
    //    Process.init

    //读取基础表
    val cellinfo = RedisUtils.getResultMap(baselibname)
    cellinfo.foreach(e => {
      val c_info = new StaticCellInfo2
      val strArr = e._2.split("\t")
      if (strArr.length == 11){
        c_info.cellid_ = e._1
        c_info.longitude_ = strArr(gc_lon).toDouble
        c_info.latitude_ = strArr(gc_lat).toDouble
        c_info.freq_ = strArr(gc_freq).toInt
        c_info.cell_pci_ = strArr(gc_freq).toInt
        c_info.in_door_ = strArr(gc_inout)

        tmpCellInfo.put(e._1, c_info)
      }
    })
    tmpCellInfo

    //读取临区表
    //    val neiinfo = RedisUtils.getResultMap(neilibname)
    //    neiinfo.foreach(e => {
    //      val key = e._1
    //      val value = e._2.toInt
    //      tmpNeiInfo.put(key, value)
    //    })
    //    (tmpCellInfo, tmpNeiInfo)
  }

  // 墨卡托转经纬度
  def Mercator2lonlat(x: Int, y: Int): (Double, Double) = {
    val lon = x / 20037508.34 * 180
    var lat = y / 20037508.34 * 180
    lat = 180 / Pi * (2 * atan(exp(lat * Pi / 180)) - Pi / 2)
    (lon, lat)
  }

  def init(): Unit ={
    val center_latitude = 23.91
    val center_longgitude = 106.62

    disper_longitude_ = calc_distance(center_longgitude - 1, center_latitude, center_longgitude + 1, center_latitude) / 2
    disper_latitude_ = calc_distance(center_longgitude, center_latitude - 1, center_longgitude, center_latitude + 1) / 2
  }

  def mapProcess(in: String): (String, Array[String]) = {
    val strArr = in.split(seprator, -1)
    if(mr_length == strArr.length){
      val imsi = strArr(index_imsi)
      (imsi, strArr)
    }else{
      ("", Array[String]())
    }
  }

  def reduceProcess(key : String, Iter : Iterable[Array[String]], cellmap : Map[String, StaticCellInfo2], neimap : imMap[String, String],
                    sgmap : imMap[String, String]): Array[(String, String)] = {

    if(key.equals("")){
      Array[(String, String)]((key,""))
    }
    else {
      init

      CellInfo = cellmap
      NeiInfo = neimap
      val vlist = Iter.toList.sortBy(_(index_time))
      var usr = new User2
      vlist.map{e =>
        val xdr = new XDR_UE_MR_S2

        xdr.cell_id_ = if (e(index_cellid) != "") e(index_cellid) else "-1"
        xdr.ta_ = if (e(index_ta) != "") e(index_ta).toInt else -1
        xdr.aoa_ = if (e(index_aoa) != "") e(index_aoa).toInt else -1
        xdr.serving_freq_ = if (e(index_sfreq) != "") e(index_sfreq).toInt else -1
        xdr.serving_rsrp_ = if (e(index_srsrp) != "") e(index_srsrp).toInt else -1
        xdr.time_ = e(index_time).replace("[\\-: ]", "").toLong

        val neinum = if (e(index_neinum) != "") e(index_neinum).toInt else 0

        var mrneiinfo = ListBuffer[(String, Int)]()
        for(i <- 0 to (neinum -1)){
          val cur = index_neinum+1 + nei_length * i
          if(e(cur+neifreq_index) != "" && e(cur+neipci_index) != "" && e(cur+neirsrp_index) != ""){
            val neipci_freq = e(cur+neipci_index) + "," + e(cur+neifreq_index)
            val neirsrp = e(cur+neirsrp_index).toInt
            mrneiinfo += ((neipci_freq, neirsrp))
          }
        }

        //        println("mrneiinfo = " + mrneiinfo)


        // 计算经纬度
        val res = locate(xdr, usr, mrneiinfo)

        usr = res._3
        //        val time: Long = xdr.time_ / 100000000
        val rsrp = xdr.serving_rsrp_

        if(res._1 == -1 || res._1 == -1){
          // 如果没有算出经纬度(广西)
          //          time.toString + "|" + "-1" + "|" + "-1" + "|" + rsrp + "|" + res._4
          ("","")
        } else{
          val mcoo = transform2Mars(res._1, res._2)
          val coo = lonLat2Mercator(mcoo._1, mcoo._2)
          val sgX = rint(coo._1 / grid_size).toInt
          val sgY = rint(coo._2 / grid_size).toInt

          val (flon, flat) = Mercator2lonlat(sgX, sgY)
          var tac = ""
          var eci = ""
          var area = ""
          sgmap.get(sgX+","+sgY) match{
            case None => None
            case Some(info) => {
              val infoArr = info.split(",", -1)
              tac = infoArr(sg_tac)
              eci = infoArr(sg_eci)
              area = infoArr(sg_area)
            }
          }

          val key1 = e(index_imsi)
          val value1 = Array[String](e(index_time), e(index_imsi), e(index_msisdn), e(index_imei), tac,
            eci, area, flon.toString, flat.toString, sgX.toString, sgY.toString).mkString(",")
          (key1, value1)
        }
      }.toArray
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

  def locate(sdata : XDR_UE_MR_S2, udata : User2, mrneiinfo: ListBuffer[(String, Int)]) : (Double, Double, User2, Int) ={
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

  def locate_by_ta_aoa(aoa : Int, ta : Int, cell_id : String) : (Double, Double, Int) ={
    val angle = aoa / 2       //aoa ta 没有ntohs
    val cellinfo_ = CellInfo.getOrElse(cell_id, new StaticCellInfo2)

    if (aoa != -1 && angle < 360 && ta != -1 && ta < 2048 && cellinfo_.cellid_ != "-1"){
      val radius = distance(ta)
      val radian = (360 - angle) * Pi / 180
      val x = cos(radian) * radius
      val y = sin(radian) * radius
      val longitude_ : Double = cellinfo_.longitude_ - y / disper_longitude_
      val latitude_ : Double = cellinfo_.latitude_ - x / disper_latitude_
      (longitude_, latitude_, 1)
    } else{
      (-1, -1, -1)
    }
  }

  def locate_by_nei_level(sdata : XDR_UE_MR_S2, mrneiinfo: ListBuffer[(String, Int)]) : (Double, Double, Int) ={
    var longitude_ : Double = -1
    var latitude_ : Double = -1
    val cellId = sdata.cell_id_
    val main_cell = CellInfo.getOrElse(cellId, new StaticCellInfo2)
    main_cell.cellid_ match{
      // 如果在字典表里没有找到 返回(-1, -1)
      case "-1" => {
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
              case "室内" => {
                // 室内
                val ll1 = GetInsideMRLonLat(main_cell)
                (ll1._1, ll1._2, 3)
              }
              case _ => {
                //　室外
                var lnei = ListBuffer[(StaticCellInfo2, Int)]()
                mrneiinfo.foreach(x => {
                  val neiinfo_cellid1 = NeiInfo.getOrElse(cellId+","+x._1.toString, "-1")
                  if(neiinfo_cellid1 != "-1"){
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
}
