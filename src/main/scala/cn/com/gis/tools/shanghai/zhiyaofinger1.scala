package cn.com.gis.tools.shanghai

import cn.com.gis.utils.tRedisPutMap
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.Map

/**
 * Created by wangxy on 15-11-16.
 */
object zhiyaofinger1 {
  val Finger_name = "gsmFingerLib1"

  def mapProcess(in: String): (String, Array[String]) = {
    val strArr = in.split(",", -1)
    if(strArr.length == 7){
      val sg = strArr.slice(1,3).mkString("|")
      val value = Array[String](strArr(3), "0", strArr(6), strArr(4), strArr(5))
      (sg, value)
    }else {
      ("-1", Array[String]())
    }
  }

  // 输出(x|y, ta, ismcell, rsrp, num)
  def reduceProcess(key: String, Iter: Iterable[Array[String]], fmap: Map[String, String]): Unit = {
    if(key != "-1"){
      val value = Iter.toList.sortBy(_(3).toInt).reverseMap(_.mkString(",")).mkString("$")
      fmap.put(key, value)
    }
  }

  def main(args: Array[String]): Unit = {
    if(args.length != 1) {
      System.err.println("Usage: <in-file>")
      System.exit(1)
    }

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    tRedisPutMap.deltable(Finger_name)

    val textRDD = sc.textFile(args(0))
    textRDD.map(mapProcess).groupByKey().foreachPartition(Iter => {
      val fmap = Map[String, String]()
      Iter.foreach(x => reduceProcess(x._1, x._2, fmap))
      tRedisPutMap.putMap2Redis(Finger_name, fmap)
    })
  }
}
