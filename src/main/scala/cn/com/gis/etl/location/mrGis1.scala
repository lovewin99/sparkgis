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

object mrGis1 {
  val propFile = "/config/mrlocation.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val which_function = prop.getOrElse("WHICH_FUNCTION", "1").toInt
  //  val ssTime = prop.getOrElse("SSTIME", "5").toInt
  //  val cpPath = prop.getOrElse("CHECKPOINT_PATH", "")
  //  val topics = prop.getOrElse("TOPIC", "")
  //  val threadNum = prop.getOrElse("THREAD_NUM", "2").toInt
  //  val outPath = prop.getOrElse("OUTPUT_PATH", "")
  //  val group = prop.getOrElse("GROUP", "")
  //  val zkQuorum = prop.getOrElse("ZKQUORUM", "")
  val isFromFirst = prop.getOrElse("IS_FROM_FIRST", "")

  val baselibname = prop.getOrElse("BSBASE_LIB", "")
  val neilibname = prop.getOrElse("BSNEI_LIB", "")
  val sglibname = prop.getOrElse("SHANGE_LIB", "")
  val fingerlibname = prop.getOrElse("FINGER_LIB", "")

  val mrlibname = prop.getOrElse("MRGIS_LIB", "")

  def main (args: Array[String]) {
    if (args.length < 7) {
      System.err.println("Usage: KafkaWordCount <ssTime> <zkQuorum> <group> <topics> <numThreads> <checkPointPath> <output>")
      System.exit(1)
    }


    val Array(ssTime, zkQuorum, group, topics, numThreads, cpPath, strPath) = args

    val conf = new SparkConf().setAppName("Streaming Test").set("spark.streaming.receiver.writeAheadLog.enable", "true")
    if("1" == isFromFirst)
      conf.set("auto.offset.reset ", "smallest")
    //    val conf = new SparkConf().setAppName("Streaming Test")
    val ssc = new StreamingContext(conf, Seconds(ssTime.toInt))
    //    val ssc = strContextSingleton.getInstance(conf, Seconds(ssTime.toInt))

    ssc.checkpoint(cpPath)
    val arrTopic = topics.split(",")
    which_function match{
      case 1 =>{
        // 广西算法
        val bsmap = gxFunction1.Setup()
        val sgmap = nRedisUtils.getMapFromRedis(sglibname)
        val neimap = nRedisUtils.getMapFromRedis(sglibname)
        val vbs = ssc.sparkContext.broadcast(bsmap)
        val vnei = ssc.sparkContext.broadcast(neimap)
        val vsg = ssc.sparkContext.broadcast(sgmap)

        arrTopic.foreach(etpc => {
          val topicMap = Map[String, Int]((etpc, numThreads.toInt))
          val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
          lines.mapPartitions(
            lrdd => {
              lrdd.map(gxFunction1.mapProcess)
            }
          ).groupByKey().mapPartitions{rdds =>
            val gisinfo = rdds.map(l => gxFunction1.reduceProcess(l._1, l._2, vbs.value, vnei.value, vsg.value))
            val gisArr = gisinfo.flatMap(x=>x).filter(_._1 != "")
            nRedisUtils.putMap2Redis(mrlibname, gisArr.toMap)
            gisArr.map(x=>x._2)
          }.saveAsTextFiles(strPath)
        })
      }
      case 2 =>{
        // 上海算法
        val bsmap = nRedisUtils.getMapFromRedis(baselibname)
        val sgmap = nRedisUtils.getMapFromRedis(sglibname)
        val neimap = nRedisUtils.getMapFromRedis(sglibname)
        val fingermap = nRedisUtils.getMapFromRedis(fingerlibname)

        val vbs = ssc.sparkContext.broadcast(bsmap)
        val vnei = ssc.sparkContext.broadcast(neimap)
        val vfinger = ssc.sparkContext.broadcast(fingermap)
        val vsg = ssc.sparkContext.broadcast(sgmap)

        arrTopic.foreach(etpc => {
          val topicMap = Map[String, Int]((etpc, numThreads.toInt))
          val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
          lines.mapPartitions(
            lrdd => {
              lrdd.map(x => shFunction1.inMapProcess(x, vnei.value, vbs.value))
            }
          ).groupByKey().mapPartitions{rdds =>
            val gisinfo = rdds.map(l => shFunction1.CombineUserInfo(l._1, l._2, vfinger.value, vsg.value))
            val gisArr = gisinfo.flatMap(x=>x).filter(_._1 != "")
            nRedisUtils.putMap2Redis(mrlibname, gisArr.toMap)
            gisArr.map(x=>x._2)
          }.saveAsTextFiles(strPath)
        })
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
