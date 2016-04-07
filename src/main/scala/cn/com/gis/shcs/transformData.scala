package cn.com.gis.shcs

/**
 * Created by wangxy on 16-4-1.
 */

import org.apache.spark.{SparkConf, SparkContext}
import java.text.SimpleDateFormat


object transformData {

  val time_index = 2
  val bcch_index = 16
  val bsic_index = 17
  val rsrpdlsup_index = 31


  def main(args: Array[String]): Unit = {
//    if(args.length != 2){
//      System.out.print("error input: <input-path> <output-path>")
//      System.exit(1)
//    }

//    val conf = new SparkConf().setAppName("gsmgis0119 Application")
//    val sc = new SparkContext(conf)
//    val rdd = sc.textFile(args(0))

//    val a = "1456104040000".toLong

    val a = "1456127100"+"000"
    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    println(sdf.format(a.toLong))

  }
}
