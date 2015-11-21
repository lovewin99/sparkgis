package cn.com.gis.etl.shanghai

import java.text.SimpleDateFormat

import com.utils.ConfigUtils
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ArrayBuffer
import scala.math._

/**
 * Created by wangxy on 15-11-21.
 */
object lvbo1 {
  val propFile = "/config/shanghai.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val grip_size = prop.getOrElse("GRID_SIZE", "25").toInt

  val in_length = 6

  val index_lonlat = 0
  val index_sg = 1
//  val index_dis = 2

  val index_time = 0
  val index_imsi = 1
  val index_sgx = 2
  val index_sgy = 3
  val index_lon = 4
  val index_lat = 5

  val dis_time = 40000
  val distance_limit = 5.0   //单位20m

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
          val imsi = strArr(index_imsi)
          (imsi, strArr)
        }
        else
          ("-1",Array[String]())
      }
      case _ => ("-1",Array[String]())
    }

    val result = mapRdd.filter(_._1 != "-1").groupByKey().map{
      case (user, info) => {
        val sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS")
        val vlist = info.toList.sortBy(_(index_time))
        var fdata = ArrayBuffer[Array[String]]()
        var tdata = ArrayBuffer[Array[String]]()
        // 用于记录20条中首条时间
        var ttime = 0L
        vlist.foreach{x =>
          // 当前条数时间
          val _time = sdf.parse(x(index_time)).getTime
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
                  ttime = sdf.parse(tdata.head(index_time)).getTime
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
                  ttime = sdf.parse(tdata.head(index_time)).getTime
                }
              }
              //              tdata += x
              //计算中心
              if(20 == tdata.length){
                var sgx = 0.0
                var sgy = 0.0
                tdata.foreach{y =>
//                  val xy = y(index_sg).split("\\|",-1)
                  sgx += y(index_sgx).toDouble
                  sgy += y(index_sgy).toDouble
                }
                sgx /= 20
                sgy /= 20
//                val xy10 = tdata(10)(index_sg).split("\\|",-1)
//                val d = sqrt(pow(xy10(0).toLong - sgx, 2) + pow(xy10(1).toLong - sgy, 2))
                val d = sqrt(pow(tdata(10)(index_sgx).toLong - sgx, 2) + pow(tdata(10)(index_sgy).toLong  - sgy, 2))
                // 如果第10条数据大于门限值 则用中心坐标更新第10条坐标
                if(d > distance_limit){
                  tdata(10).update(index_sgx, rint(sgx).toInt.toString)
                  tdata(10).update(index_sgy, rint(sgy).toInt.toString)
                }
//                  tdata(10).update(index_sg, rint(sgx).toInt + "|" + rint(sgy).toInt)

                // 去除第一条数据 更新首条数据时间
                fdata += tdata.head
                tdata = tdata.tail
                ttime = sdf.parse(tdata.head(index_time)).getTime
              }
            }
          }
        }
        fdata ++= tdata

        // 临时算距离
//        fdata.foreach{x =>
//          val sg = x(index_sg)
//          val nxy = sg.split("\\|", -1)
//          val sxy = x(index_lonlat).split("\\|", -1)
//          val d = Mercator2lonlat(nxy(0).toInt * grip_size, nxy(1).toInt * grip_size)
//          //          val tmpd = rint(sqrt(pow(d._1 - sxy(0).toDouble, 2) + pow(d._2 - sxy(1).toDouble, 2)))
//          val tmpd = calc_distance(d._1, d._2, sxy(0).toDouble, sxy(1).toDouble)
//          x.update(index_dis, tmpd.toString)
//        }
        fdata.map(_.mkString(",")).mkString("\n")
      }
    }
    result.saveAsTextFile(args(1))
  }
}
