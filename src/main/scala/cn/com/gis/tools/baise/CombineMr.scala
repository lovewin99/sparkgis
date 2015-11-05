package cn.com.gis.tools.baise

import java.text.SimpleDateFormat

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.{ListBuffer, Map}

/**
 * Created by wangxy on 15-11-4.
 */
object CombineMr {

  // 数据长度
  val mr_length = 33
  // 临区信息开始位置
  val neibegin_index = 16
  // 临区信息长度
  val neiinfo_length = 5
  // 最大临区信息组数
  val neigroup_num = 6
  // 临区信息
  val neifreq_index = 16
  val neipci_index = 17
  // cellid
  val cellid_index = 28
  // 以下三个字段标识唯一标识一个用户
  val ues1apid_index = 29
  val code_index = 30
  val groupid_index = 31
  // 测量时间(数据中带的)
  val date_index = 32
  // 测量时间间隔
  val time_interval = 8000

  // 输出字段(userid, (时间, in))
  def mapProcess(in: String, sdf: SimpleDateFormat): (String, (Long, Array[String])) = {
    val strArr = in.split("\\|", -1)
    if(mr_length == strArr.length){
      val time = sdf.parse(strArr(date_index)).getTime
      val userid = Array(strArr(ues1apid_index), strArr(code_index), strArr(groupid_index)).mkString("|")
      (userid, (time, strArr))
    }else{
      ("-1", (0L, Array[String]()))
    }
  }

//  def mapProcess(in: String): (String, (Long, Array[String])) = {
//    val strArr = in.split("\\|", -1)
//    if(mr_length == strArr.length){
//      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//      val time = sdf.parse(strArr(date_index)).getTime
//      val userid = Array(strArr(ues1apid_index), strArr(code_index), strArr(groupid_index)).mkString("|")
//      (userid, (time, strArr))
//    }else{
//      ("-1", (0L, Array[String]()))
//    }
//  }

  def reduceProcess(userid: String, iter: Iterable[(Long, Array[String])]): String = {
    if(userid != "-1"){
      var tTime = 0L
      var tCellid = ""
      var lIn = ListBuffer[String]()
      var lNei = ListBuffer[String]()
      val mNei = Map[(String, String), String]()
      var fStr = ListBuffer[String]()
      iter.toList.sortBy(_._1).foreach(x => {
        // 首次进入或者满足切换条件
        if(0L == tTime || x._2(cellid_index) != tCellid || (x._1 - tTime) > time_interval){
          if(0L != tTime){
            //非首次进入
            mNei.foreach(x => {
              lNei += x._2
            })

            lIn.foreach(x => {
              if(lNei.length >= neigroup_num)
                fStr += Array(x, lNei.slice(0, neigroup_num).mkString("|")).mkString("|")
              else{
                // 补全空临区
                for(i <- 0 to (neigroup_num - lNei.length -1)){
                  lNei += "||||"
                }
                fStr += Array(x, lNei.slice(0, neigroup_num).mkString("|")).mkString("|")
              }
            })
            lIn.clear()
            mNei.clear()
            lNei.clear()
          }
          tTime = x._1
          tCellid = x._2(cellid_index)
          lIn += x._2.mkString("|")
          if(x._2(neifreq_index) != "" && x._2(neipci_index) != "" && !mNei.contains((x._2(neifreq_index), x._2(neipci_index)))){
            mNei.put((x._2(neifreq_index), x._2(neipci_index)), x._2.slice(neifreq_index, neifreq_index + neiinfo_length).mkString("|"))
          }
        }else {
          lIn += x._2.mkString("|")
          if(x._2(neifreq_index) != "" && x._2(neipci_index) != "" && !mNei.contains((x._2(neifreq_index), x._2(neipci_index)))){
            mNei.put((x._2(neifreq_index), x._2(neipci_index)), x._2.slice(neifreq_index, neifreq_index + neiinfo_length).mkString("|"))
          }
        }
      })
      if(lIn.length != 0){
        mNei.foreach(x => {
          lNei += x._2
        })
        lIn.foreach(x => {
          if(lNei.length >= neigroup_num)
            fStr += Array(x, lNei.slice(0, neigroup_num).mkString("|")).mkString("|")
          else{
            // 补全空临区
            for(i <- 0 to (neigroup_num - lNei.length -1)){
              lNei += "||||"
            }
            fStr += Array(x, lNei.slice(0, neigroup_num).mkString("|")).mkString("|")
          }
        })
      }
      fStr.mkString("\n")
    }else{
      "-1"
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: <in-path> <out-path>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("CombineMr Application")
    val sc = new SparkContext(conf)
    val textRDD = sc.textFile(args(0))

    val result = textRDD.mapPartitions(Iter => {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      Iter.map(x => mapProcess(x, sdf))
    }).groupByKey(100).mapPartitions(Iter => Iter.map(x => reduceProcess(x._1, x._2)))

//    val result = textRDD.mapPartitions(Iter => {
//      Iter.map(mapProcess)
//    }).filter(_._1 != "-1").groupByKey().mapPartitions(Iter => Iter.map(x => reduceProcess(x._1, x._2)))

    result.saveAsTextFile(args(1))
  }

}
