package cn.com.gis.tools.shanghai

/**
 * Created by wangxy on 16-1-18.
 */

import cn.com.gis.utils.tRedisPutMap
import com.utils.ConfigUtils
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.{ArrayBuffer, Map}

import scala.math._

object lteGenFingerlib1 {
  val propFile = "/config/shanghai.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val finger_line_max_num = prop.getOrElse("FINGER_LINE_MAX_NUM", "12").toInt
  val rsrp_down_border = prop.getOrElse("RSRP_DOWN_BORDER", "-100").toInt
  val grip_size = prop.getOrElse("GRID_SIZE", "25").toInt
  val rssi_downlimit = prop.getOrElse("RSSI_DOWNLIMIT", "-100").toInt
  val rssi_uplimit = prop.getOrElse("RSSI_UPLIMIT", "-40").toInt

  // 指纹库名称
  val Finger_name = "yuanqulte1"

  // 指纹数据长度
  val Finger_length = 12
  val Sampling_index = 0
  val lon_index = 2
  val lat_index = 3
  val lac_index = 4
  val ci_index = 5
  val bcch_index = 6
  val bsic_index = 7
  val ta_index = 8
  val relevsub_index = 10
  val ismcell_index = 11

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

  // 电频值在范围内
  def rejectByRssi(info: Array[String]): Boolean = {
    info(relevsub_index) != "" && info(relevsub_index).toInt >= rssi_downlimit && info(relevsub_index).toInt <= rssi_uplimit
  }

  // 输出(采样点, 整条数据)
  // 输出(x|y,Array(标识(bcch|bsic), ta, ismain, rxlevsub)))
  def inMapProcess(in: String): (String, Array[String]) = {
    val strArr = in.split(",", -1)
    if(Finger_length == strArr.length && rejectByRssi(strArr)){
      val lon = strArr(lon_index).toDouble
      val lat = strArr(lat_index).toDouble
      val rxlevsub = strArr(relevsub_index).toInt
      if(!outOfChina(lon, lat) && rxlevsub > rsrp_down_border){
        val res1 = lonLat2Mercator(lon, lat)
        // 栅格化
        val sg = rint(res1._1 / grip_size).toInt + "|" + rint(res1._2 / grip_size).toInt
        // 标识 现在bcch|bsic (fix_me)
        val flag = strArr(bcch_index) + "|" + strArr(bsic_index)
        val value = Array[String](flag, strArr(ta_index), strArr(ismcell_index), rxlevsub.toString)
        (sg, value)
      }else{
        ("-1", Array[String]())
      }
    }else {
      ("-1", Array[String]())
    }
  }

  def inReduceProcess(sg: String, Iter: Iterable[Array[String]], fmap: Map[String, String]): Unit = {
    if(sg != "-1") {
      // 安标识将相同纹线信息合并平均
      var lineArr2 = ArrayBuffer[ArrayBuffer[String]]()
      Iter.toList.groupBy(_(0)).foreach(x => {
        var lineArr1 = ArrayBuffer[String]()
        val sum = x._2.size
        val mRsrp = x._2./:(0) { (x, y) => x + y(3).toInt } / sum
        // 如果有主小区存在 取主小区标识 (flag, ta, ismcell)
        lineArr1 ++= x._2.sortBy(_(2)).reverse.head.slice(0, 3)
        //        println("linearr1(0)="+x._2.sortBy(_(2)).reverse.head.slice(0, 3).mkString("$"))
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
        //        val fstr = fingerInfo.map(_.mkString(",")).mkString("$")
        //        fmap.put(sg, fstr)
      } else {
        //        val n = finger_line_max_num - fingerInfo.length - neiInfo.length
        //        fingerInfo ++= neiInfo.slice(0, finger_line_max_num - fingerInfo.length)
        fingerInfo ++= neiInfo
        val fstr = fingerInfo.sortBy(_(3).toInt).reverse.slice(0, finger_line_max_num).map(_.mkString(",")).mkString("$")
        //        for (i <- 0 to (n - 1)) {
        //          fingerInfo += ArrayBuffer(",,,,")
        //        }
        //        val fstr = fingerInfo.map(_.mkString(",")).mkString("$")
        fmap.put(sg, fstr)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Usage: <in-file>")
      System.exit(1)
    }

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val textRDD = sc.textFile(args(0))

    tRedisPutMap.deltable(Finger_name)

    textRDD.mapPartitions(Iter=> Iter.map(inMapProcess)).groupByKey().foreachPartition(Iter => {
      val fmap = Map[String, String]()
      Iter.foreach(x => inReduceProcess(x._1, x._2, fmap))
      tRedisPutMap.putMap2Redis(Finger_name, fmap.toMap)
      //      fmap.foreach(println)
    })
  }
}
