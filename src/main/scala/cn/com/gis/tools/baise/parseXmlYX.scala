package cn.com.gis.tools.baise

import cn.com.utils.XmlInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by wangxy on 15-8-6.
 */
object parseXmlYX {

  def parseXML1(content: String) ={
    val a = xml.XML.loadString(content)
    // eNBId
    val eNBId = (a \ "eNB" \ "@id" text).toString

    val columns = List("@id", "@MmeUeS1apId", "@MmeCode", "@MmeGroupId")
    val m33 = "MR.LteScRSRP MR.LteScRSRQ MR.LteScTadv MR.LteSceNBRxTxTimeDiff MR.LteScPHR MR.LteScAOA MR.LteScSinrUL MR.LteScEarfcn MR.LteScPci MR.LteNcRSRP MR.LteNcRSRQ MR.LteNcEarfcn MR.LteNcPci MR.TdsPccpchRSCP MR.TdsNcellUarfcn MR.TdsCellParameterId MR.GsmNcellBcch MR.GsmNcellCarrierRSSI MR.GsmNcellNcc MR.GsmNcellBcc MR.LteScPUSCHPRBNum MR.LteScPDSCHPRBNum MR.LteScBSR MR.LteScRI1 MR.LteScRI2 MR.LteScRI4 MR.LteScRI8"

    // <measurement>...</measurement>
    // and smr == m33
    val c = (a \\ "measurement").filter(x => (x \ "smr" text) == m33) \\ "object"
    c.flatMap{ x =>
      val columnsValue = columns.map(x \ _ text).mkString("|")  // id MmeUeS1apId MmeCode MmeGroupId MmeGroupId TimeStamp
      val timeValue = (x \ "@TimeStamp" text).replaceAll("(\\d{4}-\\d{2}-\\d{2})T(\\d{2}:\\d{2}:\\d{2})\\.\\d+", "$1 $2")  // timestamp

      (x \ "v").map{ x =>
        val ret = ArrayBuffer[String]()
        ret += (x text).trim.replace(" ", "|").replace("NIL", "")  // v
        ret += (eNBId, columnsValue, timeValue)
        ret.mkString("|")
      }.toList
    }
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


    val result = sc.newAPIHadoopFile[LongWritable, Text, XmlInputFormat](srcPath).mapPartitions{x =>
      x.flatMap{y =>parseXML1(y._2.toString)}
    }
    val array = Array(7,8,0,1,2,4,5,6,23,24,25,26,3,22,20,21,11,12,9,10,14,15,13,18,19,16,17,27,28,29,30,31,32)
    result.map { x =>
      val xs = x.split("\\|")
      val ab = new ArrayBuffer[String]()
      for(i <- array) {
        ab += xs(i)
      }
      ab.mkString("|")
    }.saveAsTextFile(tarPath)

  }

}
