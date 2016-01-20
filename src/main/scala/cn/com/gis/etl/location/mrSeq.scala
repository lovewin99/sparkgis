package cn.com.gis.etl.location

/**
 * Created by wangxy on 16-1-5.
 */

import com.utils.ConfigUtils
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import function.{gxFunction1,shFunction1}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import cn.com.gis.utils.nRedisUtils

object mrSeq {
  val propFile = "/config/mrlocation.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val isFromFirst = prop.getOrElse("IS_FROM_FIRST", "")
  // 随机栅格表名
  val sgRname = prop.getOrElse("SG_RANDOM_NAME", "")

  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      System.err.println("Usage: KafkaWordCount <ssTime> <zkQuorum> <group> <topics> <numThreads> <checkPointPath> <output>")
      System.exit(1)
    }


    val Array(ssTime, zkQuorum, group, topics, numThreads, cpPath, strPath) = args

    val conf = new SparkConf().setAppName("Streaming Test").set("spark.streaming.receiver.writeAheadLog.enable", "true")
    if("1" == isFromFirst)
      conf.set("auto.offset.reset ", "smallest")
    val ssc = new StreamingContext(conf, Seconds(ssTime.toInt))

    ssc.checkpoint(cpPath)
    val arrTopic = topics.split(",")

    val sgRmap = nRedisUtils.getMapFromRedis(sgRname)
    val vsgR = ssc.sparkContext.broadcast(sgRmap)

    arrTopic.foreach(etpc => {
//      val topicMap = Map[String, Int]((etpc, numThreads.toInt))
//      val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
//      lines.mapPartitions(
//        lrdd => {
//          lrdd.map(gxFunction1.mapProcess)
//        }
//      ).groupByKey().mapPartitions{rdds =>
//        val gisinfo = rdds.map(l => gxFunction1.reduceProcess(l._1, l._2, vbs.value, vnei.value, vsg.value))
//        val gisArr = gisinfo.flatMap(x=>x).filter(_._1 != "")
//        nRedisUtils.putMap2Redis(mrlibname, gisArr.toMap)
//        gisArr.map(x=>x._2)
//      }.saveAsTextFiles(strPath)
    })
  }
}
