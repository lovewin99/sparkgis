package cn.com.gis.etl

/**
 * Created by wangxy on 15-8-5.
 */

//import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.spark.{SparkContext, SparkConf}
import SparkContext._

//import cn.com.gis.tools.{sparkInstance}
import cn.com.gis.etl.function.Process1


object CaculateGis1 {

  def main(args : Array[String]): Unit ={
    if (args.length != 2) {
      System.err.println("Usage: <in-file> <out-file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("sparkgis Application")
    val sc = new SparkContext(conf)

    val textRDD = sc.textFile(args(0))

    val xmap = Process1.Setup
    val x = sc.broadcast(xmap._1)
    val y = sc.broadcast(xmap._2)

    val result = textRDD.mapPartitions(Iter => {
      Iter.map(e => Process1.mapProcess(e))
    }).groupByKey().map(e => Process1.reduceProcess(e._1, e._2, x.value, y.value))


    result.saveAsTextFile(args(1))
  }
}
