package cn.com.gis.tools.shanghai

/**
 * Created by wangxy on 15-10-30.
 */

import org.apache.spark.{SparkContext, SparkConf}
import com.utils.RedisUtils
import scala.collection.mutable.Map

/**
 * Created by wangxy on 15-9-1.
 */
object GenFingerLib {

  val Finger_cellid = 4
  // 服务小区所在字段序号
  val Finger_length = 18
  // 指纹数据长度
  val Table_Name = "FingerInfo" // 指纹库表名

  def mapProcess(in: String): (String, String) = {
    val strArr = in.split("\t")
    var key = "-1"
    var value = "-1"
    if (Finger_length == strArr.length) {
      key = strArr(Finger_cellid)
      value = in.replaceAll("\t", "|")
    }
    (key, value)
  }

  //  def ruduceProcess(key : String, Iter : Iterable[String]):Unit={
  //    if("-1" != key){
  //      val fingerinfo = Map[String, String]()
  //      var value = ""
  //      var bFlag = true
  //      Iter.foreach(x =>{
  //        if(bFlag){
  //          value = x
  //          bFlag = false
  //        }else{
  //          value += "$"
  //          value += x
  //        }
  //      })
  //      fingerinfo.put(key, value)
  //      RedisUtils.putMap2RedisTable(Table_Name, fingerinfo)
  //    }
  //  }

  def ruduceProcess(key: String, Iter: Iterable[String]): (String, String) = {
    var value = ""
    if ("-1" != key) {
      var bFlag = true
      Iter.foreach(x => {
        if (bFlag) {
          value = x
          bFlag = false
        } else {
          value += "$"
          value += x
        }
      })
    }
    (key, value)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Usage: <in-file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("GenFingerLib Application")
    val sc = new SparkContext(conf)
    val textRDD = sc.textFile(args(0))

    RedisUtils.delTable(Table_Name)

    val fingerinfo = Map[String, String]()

    val result = textRDD.mapPartitions(Iter => Iter.map(mapProcess)).groupByKey().mapPartitions(Iter => {
      Iter.map(x => ruduceProcess(x._1, x._2))
    }).collect

    result.foreach(x => fingerinfo.put(x._1, x._2))

    RedisUtils.putMap2RedisTable(Table_Name, fingerinfo)
    //    result.saveAsTextFile(args(1))
  }

  //  def main(args : Array[String]): Unit ={
  //    if (args.length != 1) {
  //      System.err.println("Usage: <in-file>")
  //      System.exit(1)
  //    }
  //
  //    val conf = new SparkConf().setAppName("GenFingerLib Application")
  //    val sc = new SparkContext(conf)
  //    val textRDD = sc.textFile(args(0))
  //
  //    RedisUtils.delTable(Table_Name)
  //
  //    val fingerinfo = Map[String, String]()
  //
  //    val result = textRDD.mapPartitions(Iter => Iter.map(mapProcess)).groupByKey().mapPartitions(Iter =>
  //      Iter.map(x=> ruduceProcess(x._1, x._2))).collect
  //
  //    RedisUtils.putMap2RedisTable(Table_Name, fingerinfo)
}
