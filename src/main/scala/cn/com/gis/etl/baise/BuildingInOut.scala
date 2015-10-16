package cn.com.gis.etl.baise

import com.utils.RedisUtils
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by wangxy on 15-10-16.
 */
object BuildingInOut {

  val index_length = 9
  val index_time = 0
  val index_userkey = 1
  val index_x = 2
  val index_y = 3
  val index_cellid = 4
  val index_rsrp = 5
  val index_neirsrp = 6
  val index_flag = 7

  val border_rsrp = 46
  val change_rsrp = 10

  val indoor = "0"
  val outdoor = "1"
  val unknow = "2"

  def MapProcess(in : String): (String, Array[String]) = {
    val strArr = in.split(",", -1)
    if(strArr.length == index_length){
      (strArr(index_userkey), strArr)
    } else{
      ("-1", Array[String]())
    }
  }

  def ReduceProcess(userKey: String, Iter: Iterable[Array[String]], buildingInfo: Map[String, String]) : String = {
    var finalstr = "-1"
    if(userKey != "-1"){
      val vList = Iter.toList.sortBy(_(index_time))
      var tmpCellid = "-1"
      var tmpFlag = "-1"
      var tmpRsrp = 0
      var isFirst = true
      val a = vList.map(x => {
        tmpCellid = x(index_cellid)
        tmpFlag = x(index_flag)
        tmpRsrp = x(index_rsrp).toInt
        if(x(index_flag) == indoor){
          // 已标记为室内
          isFirst = false
          x.mkString(",") + "," + "1"
        }else{
          val mXY = x(index_x) + "," + x(index_y)
          val isIntable = buildingInfo.getOrElse(mXY, "0")
          if (isIntable == "0" && x(index_flag) == unknow){
            // 栅格内无建筑 标记室外
            isFirst = false
            tmpFlag = outdoor
            x.update(index_flag, outdoor)
            x.mkString(",") + "," + "2"
          } else {
            // 栅格内有建筑
            if(x(index_rsrp) == "-1" || x(index_neirsrp) == "-1") // rsrp为空则舍弃这条数据
              x.mkString(",") + "," + "-1"
            else if(x(index_rsrp).toInt <= border_rsrp || x(index_neirsrp).toInt <= border_rsrp) {
              // 满足弱覆盖标准 标记为室内
              isFirst = false
              tmpFlag = x(index_flag)
              x.update(index_flag, indoor)
              x.mkString(",") + "," + "3"
            } else{
              // 不满足弱覆盖标准
              if(x(index_time).length != 14) // 时间信息有误 舍弃数据
                x.mkString(",") + "," + "-2"
              else{
                val hour = x(index_time).substring(8, 10).toInt
                if((hour >= 0 && hour <= 6) || (hour >= 22 && hour <= 24)){
                  // 大群基本精致时间段 标记为室内
                  tmpFlag = x(index_flag)
                  isFirst = false
                  x.update(index_flag, indoor)
                  x.mkString(",") + "," + "4"
                } else {
                  // 活动时间段
                  if(isFirst){
                    // 首条记录
                    tmpCellid = x(index_cellid)
                    tmpFlag = x(index_flag)
                    tmpRsrp = x(index_rsrp).toInt
                    isFirst = false
                    x.mkString(",") + "," + "-3"
                  } else{
                    if(tmpCellid != x(index_cellid)){
                      // 不是同一小区
                      tmpCellid = x(index_cellid)
                      tmpFlag = x(index_flag)
                      tmpRsrp = x(index_rsrp).toInt
                      x.mkString(",") + "," + "-4"
                    } else {
                      // 是同一小区
                      val chazhi = x(index_rsrp).toInt - tmpRsrp
                      if(chazhi >= change_rsrp){
                        // 正变化
                        tmpFlag = outdoor
                        x.update(index_flag, outdoor)
                        x.mkString(",") + "," + "5"
                      } else if (chazhi <= (0 - change_rsrp)){
                        // 负变化
                        tmpFlag = indoor
                        x.update(index_flag, indoor)
                        x.mkString(",") + "," + "6"
                      } else{
                        // 无明显变化
                        x.update(index_flag, tmpFlag)
                        x.mkString(",") + "," + "7"
                      }
                    }
                  }
                }
              }
            }
          }
        }


      })
      finalstr = a.mkString("\n")
    }
    finalstr
  }

  def main(args : Array[String]): Unit ={
    if (args.length != 2) {
      System.err.println("Usage: <in-file> <out-file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("BuildingInOut Application")
    val sc = new SparkContext(conf)

    val textRDD = sc.textFile(args(0))

    val buildinginfo = RedisUtils.getResultMap("buildinginfo")

    val info = sc.broadcast(buildinginfo)

    val result = textRDD.mapPartitions(Iter => Iter.map(MapProcess)).groupByKey().mapPartitions(Iter => Iter.map(x =>
                    ReduceProcess(x._1, x._2, info.value))).filter(_ != "-1")


    result.saveAsTextFile(args(1))
  }

}
