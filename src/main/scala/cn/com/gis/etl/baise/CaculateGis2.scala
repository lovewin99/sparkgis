package cn.com.gis.etl.baise

/**
 * Created by wangxy on 15-10-21.
 */

import org.apache.spark.{SparkConf, SparkContext}

import cn.com.gis.etl.baise.function. Process2

object CaculateGis2 {
  def main(args : Array[String]): Unit ={
    if (args.length != 2) {
      System.err.println("Usage: <in-file> <out-file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("sparkgis Application")
    val sc = new SparkContext(conf)

    val textRDD = sc.textFile(args(0))

    val xmap = Process2.Setup
    val x = sc.broadcast(xmap._1)
    val y = sc.broadcast(xmap._2)
    println("!!!!!!!!!!!!!!!!!!!!!size=================>"+xmap._2.size)

    val result = textRDD.mapPartitions(Iter => {
      Iter.map(e => Process2.mapProcess(e))
    }).groupByKey().map(e => Process2.reduceProcess(e._1, e._2, x.value, y.value))


    result.saveAsTextFile(args(1))
  }
}
