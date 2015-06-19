package cn.com.gis.etl

import org.apache.spark.{SparkContext, SparkConf}
import SparkContext._

//import cn.com.gis.tools.{sparkInstance}
import cn.com.gis.etl.function.Process


/**
 * Created by wangxy on 15-6-11.
 */
object CaculateGis {

//  var CellInfo = Map[Int, StaticCellInfo]()
//  var NeiInfo = Map[String, Int]()              //["cellid,pci_freq", cellid(临区的)]
//
//  def Setup(): Unit ={
//    // 初始化计算位置需要的参数
//    Process.init
//
//    //读取基础表
//    val cellinfo = RedisUtils.getResultMap("表名1")
//    cellinfo.foreach(e => {
//      val c_info = new StaticCellInfo
//      val strArr = e._2.split(',')
//      c_info.cellid_ = e._1.toInt
//      c_info.longitude_ = strArr(2).toDouble
//      c_info.latitude_ = strArr(3).toDouble
//      c_info.freq_ = strArr(7).toInt
//      c_info.cell_pci_ = strArr(8).toInt
//      c_info.in_door_ = strArr(9).toInt
//      c_info.azimuth_ = strArr(10).toInt
//
//      CellInfo.put(e._1.toInt, c_info)
//    })
//
//    //读取临区表
//    val neiinfo = RedisUtils.getResultMap("表名2")
//    neiinfo.foreach(e => {
//      val key = e._1
//      val value = e._2.toInt
//      NeiInfo.put(key, value)
//    })
//  }


  def main(args : Array[String]): Unit ={
    if (args.length != 2) {
      System.err.println("Usage: <in-file> <out-file>")
      System.exit(1)
    }

//    Setup()

//    val sc = sparkInstance.getInstance

//    val sparkConf = new SparkConf().setAppName("sparkgis Application")
//    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    sparkConf.registerKryoClasses(Array(classOf[cn.com.gis.tools.User],
//      classOf[cn.com.gis.tools.StaticCellInfo],
//      classOf[cn.com.gis.tools.XDR_UE_MR_S]))

    val conf = new SparkConf().setAppName("sparkgis Application")
    val sc = new SparkContext(conf)
    val textRDD = sc.textFile(args(0))

    val xmap = Process.Setup
    val x = sc.broadcast(xmap._1)
    val y = sc.broadcast(xmap._2)

    val result = textRDD.mapPartitions(Iter => {
      Iter.map(e => Process.mapProcess(e))
    }).groupByKey().map(e => Process.reduceProcess(e._1, e._2, x.value, y.value))


    result.saveAsTextFile(args(1))
  }
}
