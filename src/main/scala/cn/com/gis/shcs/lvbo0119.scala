package cn.com.gis.shcs

import java.text.SimpleDateFormat

import com.utils.ConfigUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.math._

/**
 * spark-submit --master spark://cloud138:7077 --executor-memory 8g --jars redisclient_2.10-2.12.jar,commons-pool-1.5.6.jar --driver-class-path redisclient_2.10-2.12.jar:commons-pool-1.5.6.jar --class cn.com.gis.etl.shanghai.lvbo0119 sparkgis_2.10-1.0.jar wxy/gsmout1 wxy/shlvbo0130
 * Created by wangxy on 16-1-19.
 */
object lvbo0119 {
  val propFile = "/config/shanghai.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val grip_size = prop.getOrElse("GRID_SIZE", "25").toInt

  val in_length = 6

  val time_index = 0
  val imsi_index = 5
//  val srclon_index = 2
//  val srclat_index = 3
  val nlon_index = 1
  val nlat_index = 2
  val x_index = 3
  val y_index =4
//  val dis_index = 8

  val dis_time = 40000
  val distance_limit = 5.0   //单位20m

//  // 计算两经纬度距离
//  def calc_distance(lon1 : Double, lat1 : Double, lon2 : Double, lat2 : Double) : Double={
//    val Rc = 6378137.00  // 赤道半径
//    val Rj = 6356725     // 极半径
//
//    val radLo1 = lon1 * Pi / 180
//    val radLa1 = lat1 * Pi / 180
//    val Ec1 = Rj + (Rc - Rj) * (90.0 - lat1) / 90.0
//    val Ed1 = Ec1 * cos(radLa1)
//
//    val radLo2 = lon2 * Pi / 180
//    val radLa2 = lat2 * Pi / 180
//
//    val dx = (radLo2 - radLo1) * Ed1
//    val dy = (radLa2 - radLa1) * Ec1
//    val dDeta = sqrt(dx * dx + dy * dy)
//    dDeta
//  }

  // 墨卡托转经纬度
  def Mercator2lonlat(x: Int, y: Int): (Double, Double) = {
    val lon = x / 20037508.34 * 180
    var lat = y / 20037508.34 * 180
    lat = 180 / Pi * (2 * atan(exp(lat * Pi / 180)) - Pi / 2)
    (lon, lat)
  }

  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      System.out.print("error input: <input-path> <output-path>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("lvbo Application")
    val sc = new SparkContext(conf)

    val textRdd = sc.textFile(args(0)).filter(_ != "-1|-1")

    // 输入(lon|lat, x|y, distance, sampling, time)
    val mapRdd =textRdd.map{
      case in: String => {
        val strArr = in.split(",", -1)
        if(strArr.length == in_length){
          (strArr(imsi_index), strArr)
        }
        else
          ("-1",Array[String]())
      }
      case _ => ("-1",Array[String]())
    }

    val result = mapRdd.filter(_._1 != "-1").groupByKey().map{
      case (user, info) => {
        val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
        val vlist = info.toList.sortBy(_(time_index))
        var fdata = ArrayBuffer[Array[String]]()
        var tdata = ArrayBuffer[Array[String]]()
        // 用于记录20条中首条时间
        var ttime = 0L
        vlist.foreach{x =>
          // 当前条数时间
          val _time = sdf.parse(x(time_index)).getTime
          tdata.length match{
            // 首条数据直接记录
            case 0 =>{
              tdata += x
              ttime = _time
            }
            // 不满19条时处理情况
            case len if len < 19 =>{
              tdata += x
              if(abs(_time - ttime) > dis_time){
                // 把大于阈值的条数 放到最终输出的数组中 更新首条数据时间
                while(abs(_time - ttime) > dis_time){
                  fdata += tdata.head
                  tdata = tdata.tail
                  ttime = sdf.parse(tdata.head(time_index)).getTime
                }
              }
            }
            // 当满足19条情况 计算重心点 如果中间第10条数据与中心距离超过阈值 则替换第十条位置 同时将首条拿出来
            case _ => {
              // 安小于20条逻辑计算
              tdata += x
              if(abs(_time - ttime) > dis_time){
                // 把大于阈值的条数 放到最终输出的数组中 更新首条数据时间
                while(tdata.length > 1 && abs(_time - ttime) > dis_time){
                  fdata += tdata.head
                  tdata = tdata.tail
                  ttime = sdf.parse(tdata.head(time_index)).getTime
                }
              }
              //              tdata += x
              //计算中心
              if(20 == tdata.length){
                println("!!!!!!!1")
                var sgx = 0.0
                var sgy = 0.0
                tdata.foreach{y =>
                  sgx += y(x_index).toDouble
                  sgy += y(y_index).toDouble
                }
                sgx /= 20
                sgy /= 20
                val d = sqrt(pow(tdata(10)(x_index).toLong - sgx, 2) + pow(tdata(10)(y_index).toLong - sgy, 2))
                // 如果第10条数据大于门限值 则用中心坐标更新第10条坐标
                if(d > distance_limit){
                  tdata(10).update(x_index, rint(sgx).toInt.toString)
                  tdata(10).update(y_index, rint(sgy).toInt.toString)
                  val nowlonlat = fingergis0119.Mercator2lonlat(rint(sgx).toInt*grip_size, rint(sgy).toInt*grip_size)
                  tdata(10).update(nlon_index, nowlonlat._1.toString)
                  tdata(10).update(nlat_index, nowlonlat._2.toString)
                }

                // 去除第一条数据 更新首条数据时间
                fdata += tdata.head
                tdata = tdata.tail
                ttime = sdf.parse(tdata.head(time_index)).getTime
              }
            }
          }
        }
        fdata ++= tdata

//        // 临时算距离
//        fdata.foreach{x =>
//          val d = Mercator2lonlat(x(x_index).toInt * grip_size, x(y_index).toInt * grip_size)
//          //          val tmpd = rint(sqrt(pow(d._1 - sxy(0).toDouble, 2) + pow(d._2 - sxy(1).toDouble, 2)))
//          val tmpd = calc_distance(d._1, d._2, x(srclon_index).toDouble, x(srclat_index).toDouble)
//          x.update(dis_index, tmpd.toString)
//        }
        fdata.map(_.mkString(",")).mkString("\n")
      }
    }
    result.saveAsTextFile(args(1))
  }
}
