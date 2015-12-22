package cn.com.gis.etl.test

/**
 * Created by wangxy on 15-12-21.
 */

import cn.com.gis.utils.tRedisPutMap
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object broad {
  def main(args: Array[String]) :Unit={

    if(args.length != 3){
      System.err.println("Usage: <dir> <out-file> <checkpoint>")
      System.exit(1)
    }

    //    val serverIP = args(0)
    //    val serverPort = args(1).toInt

    val Array(dir, out, ck) = args

    val conf = new SparkConf()
    conf.setAppName("wcStreaming")

    val ssc = new StreamingContext(conf, Seconds(5))

    val lines =ssc.textFileStream(dir)
    //    val lines = ssc.socketTextStream(serverIP, serverPort)

    val mapinfo = tRedisPutMap.getMapFromRedis("testbroad")
//    println(s"mapinfo.value=${mapinfo.get("key1")}")

    val m = ssc.sparkContext.broadcast(mapinfo)

//    val result = lines.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_+_)

    val result = lines.mapPartitions{l =>
      println(s" !!!!!!!!! m.value=${m.value.get("key1")}")
      l.flatMap(_.split(" "))
    }.map(word => (word, 1)).reduceByKey(_+_)
    result.saveAsTextFiles(out)

    ssc.checkpoint(ck)

    ssc.start
    ssc.awaitTermination

  }
}
