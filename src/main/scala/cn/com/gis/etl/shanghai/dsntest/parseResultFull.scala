package cn.com.gis.etl.shanghai.dsntest

import java.util.regex.Pattern

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by wangxy on 15-8-6.
 */
object parseResultFull {   // 列分隔符
  private val mmeSep = Pattern.compile("\\|")                                      // 列分隔符

  def main(args: Array[String]):Unit={
    if (args.length != 3) {
      System.err.println("Usage: <mmein-file> <mrin-file> <out-file>")
      System.exit(1)
    }

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val Array(mmeInFile, mrInFile, outFile) = args

    // "dsn/mmeoutput"
    val mmeFile = sc.textFile(mmeInFile)
    val mmeData = mmeFile.mapPartitions{ iter =>
      iter.map{ x =>
        val arr = x.split(",")
        ((arr(0), arr(1), arr(2)),arr(4).replace(")",""))
      }
    }

    // "dsn/mroutput"
    val mrFile = sc.textFile(mrInFile)
    val mrData = mrFile.mapPartitions{iter =>
      iter.map{ x =>
        val arr = x.split(",", 4)
        ((arr(0), arr(1), arr(2)), arr(3))
      }
    }


    // "dsn/result"
    mmeData.rightOuterJoin(mrData).mapPartitions{ iter =>
      iter.map{
        case ((k1, k2, k3), (x, y)) => val str = Array(k1, k2, k3, x.getOrElse(""), y).mkString(",")
      }
    }.repartition(5).saveAsTextFile(outFile)

  }
}


