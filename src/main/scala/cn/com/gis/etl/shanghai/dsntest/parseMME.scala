package cn.com.gis.etl.shanghai.dsntest

import java.util.regex.Pattern
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by wangxy on 15-8-6.
 */
object parseMME {   // 列分隔符
  private val mmeSep = Pattern.compile("\\|")                                      // 列分隔符

  def main(args: Array[String]):Unit={
    if (args.length != 2) {
      System.err.println("Usage: <in-file> <out-file>")
      System.exit(1)
    }

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val mmeFile = sc.textFile("dsn/s1ap")
    val mmeData = mmeFile.map{
        case mme(mmeUserId, mmeGroupId, mmeCode, startTime, "1") => (("","",""),("",""))
        case mme(mmeUserId, mmeGroupId, mmeCode, startTime, imsi) => ((mmeUserId, mmeGroupId,mmeCode), (startTime, imsi))
        case _ => (("","",""),("",""))
    }.filter(_ != (("","",""),("","")))

    val sortData = mmeData.groupByKey(100).mapPartitions{ iter =>
      iter.map{
        case ((mmeUserId, mmeGroupId,mmeCode), values) =>
          val temp = values.toList.sortBy(_._1).last
          Seq(mmeUserId, mmeGroupId,mmeCode, temp).mkString(",")
      }
    }

    sortData.repartition(50).saveAsTextFile("dsn/mmeoutput")


  }

  // object mme
  object mme{
    def unapply(str: String) = {
      val arr = mmeSep.split(str)
      arr.length match {
          // mmeues1apid mmegroupid mmecode starttime imsi
        case 169 => Some(arr(18), arr(22), arr(23), arr(9), arr(5))
        case _   => None
      }
    }
  }
}


