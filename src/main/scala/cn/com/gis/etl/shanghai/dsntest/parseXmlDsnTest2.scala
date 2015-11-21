package cn.com.gis.etl.shanghai.dsntest

import java.util.regex.Pattern

import cn.com.utils.XmlInputFormat
import com.utils.ConfigUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by wangxy on 15-8-6.
 */
object parseXmlDsnTest2 {
  private val conf = ConfigUtils.getConfig("/config/dsn.dsntest.properties")  // 配置文件
  private val allColumnsLab = conf.getOrElse("all.columns","")                // 所有列定义
  private val clos = conf.getOrElse("value.columns","")                    // 所需字段列
  private val sep = Pattern.compile(" ")                                      // 列分隔符
  private val mmeSep = Pattern.compile(",")                                      // 列分隔符
  val indexArray = getValueColumnIndex()

  def parseXML1(content: String) ={
    val a = xml.XML.loadString(content)
    // <measurement>...</measurement>
    // and smr == allColumnsLab
    val c = (a \\ "measurement").filter(x => (x \ "smr" text) == allColumnsLab) \\ "object"
    c.map{ x =>
      val mmeId      = (x \ "@MmeUeS1apId" text)
      val mmeGroupId = (x \ "@MmeGroupId" text)
      val mmeCode    = (x \ "@MmeCode" text)
      val timeValue  = (x \ "@TimeStamp" text).replaceAll("(\\d{4}-\\d{2}-\\d{2})T(\\d{2}:\\d{2}:\\d{2})\\.\\d+", "$1 $2")  // timestamp
      val eNBId      = (x \ "@id" text).split(":", 2)(0)

      val ret2 = ArrayBuffer[String]()
      ret2 += (mmeId, mmeGroupId, mmeCode, timeValue, eNBId)
      val ret  = ArrayBuffer[String]()
      (x \ "v").foreach{ x =>
        val tempColumns = sep.split((x text).trim)
        val aa = indexArray.map(tempColumns(_))
        ret.clear()
        ret += (aa(0), aa(1))
        if (aa(2) != "NIL" && aa(3) != "NIL")
          ret2 += aa(2) + "|" + aa(3)
      }
      if (ret(1) != "NIL") {
        ret2 patch (5, ret, 5)
        val arr = ret2.splitAt(3)
        (arr._1.mkString(","), arr._2.mkString(","))
      } else
        ("","")
    }
  }

  // 返回所需列的索引
  def getValueColumnIndex() ={
    val allMap = sep.split(allColumnsLab).zipWithIndex.toMap
    sep.split(clos).map(allMap.getOrElse(_, 0))
  }

  def main(args: Array[String]):Unit={
    if (args.length != 2) {
      System.err.println("Usage: <in-file> <out-file>")
      System.exit(1)
    }

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val Array(srcPath, tarPath) = args

    sc.hadoopConfiguration.set("xmlinput.start", "<bulkPmMrDataFile>")
    sc.hadoopConfiguration.set("xmlinput.end", "</bulkPmMrDataFile>")


    val mrData = sc.newAPIHadoopFile[LongWritable, Text, XmlInputFormat](srcPath).mapPartitions{x =>
      x.flatMap{y =>parseXML1(y._2.toString)}
    }.filter{
      case ("","") => false
      case _ => true
    }

      //.saveAsTextFile(tarPath)

    val mmeFile = sc.textFile("dsn/mmeoutput")

    val mmeData = mmeFile.map{
      _ match {
        case mme(mmeUserId, mmeGroupId, mmeCode, imsi) => (Seq(mmeUserId, mmeGroupId,mmeCode).mkString(","), imsi)
        case _ => ("", "")
      }
    }

    mrData.join(mmeData).mapPartitions{ iter =>
      val ab = ArrayBuffer[String]()
      iter.map{
        case (key, (x, y)) => ab += Seq(y, x).mkString(",")
      }
    }.saveAsTextFile(tarPath)

  }


  // object mme
  object mme{
    def unapply(str: String) = {
      val arr = mmeSep.split(str)
      arr.length match {
        case 5 => Some(arr(0), arr(1), arr(2), arr(4).replace(")",""))
        case _   => None
      }
    }
  }
}


