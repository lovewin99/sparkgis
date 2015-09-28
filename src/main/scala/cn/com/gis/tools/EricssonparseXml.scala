package cn.com.gis.tools

/**
 * Created by wangxy on 15-9-28.
 */

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

import cn.com.utils.XmlInputFormat

object EricssonparseXml {
  def parseXML1(content: String): String ={
    val a = xml.XML.loadString(content)
    // eNBId
    val eNBId = (a \ "eNB" \ "@id" text).toString

    val columns = List("@id", "@MmeUeS1apId", "@MmeCode", "@MmeGroupId")
    val m33 = "MR.LteScRSRP MR.LteNcRSRP MR.LteScRSRQ MR.LteNcRSRQ MR.LteScTadv MR.LteScPHR MR.LteScAOA MR.LteScSinrUL MR.LteScUeRxTxTD MR.LteSceEuRxTxTD MR.LteScEarfcn MR.LteScPci MR.LteScCgi MR.LteNcEarfcn MR.LteNcPci MR.LteFddNcRSRP MR.LteFddNcRSRQ MR.LteFddNcEarfcn MR.LteFddNcPci MR.LteTddNcRSRP MR.LteTddNcRSRQ MR.LteTddNcEarfcn MR.LteTddNcPci MR.LteScRI1 MR.LteScRI2 MR.LteScRI4 MR.LteScRI8 MR.LteSceNBRxTxTimeDiff MR.LteScRTTD MR.LteScPUSCHPRBNum MR.LteScPDSCHPRBNum"
    //    val m24 = "MR.LteScPlrULQci1 MR.LteScPlrULQci2 MR.LteScPlrULQci3 MR.LteScPlrULQci4 MR.LteScPlrULQci5 MR.LteScPlrULQci6 MR.LteScPlrULQci7 MR.LteScPlrULQci8 MR.LteScPlrULQci9 MR.LteScPlrDLQci1 MR.LteScPlrDLQci2 MR.LteScPlrDLQci3 MR.LteScPlrDLQci4 MR.LteScPlrDLQci5 MR.LteScPlrDLQci6 MR.LteScPlrDLQci7 MR.LteScPlrDLQci8 MR.LteScPlrDLQci9"
    //    val m7 = "MR.LteScRIP"

    // <measurement>...</measurement>
    // and smr == m33
    val c = (a \\ "measurement").filter(x => (x \ "smr" text) == m33) \\ "object"
    c.map{ x =>
      val columnsValue = columns.map(x \ _ text).mkString("|")  // id MmeUeS1apId MmeCode MmeGroupId MmeGroupId TimeStamp
      val timeValue = (x \ "@TimeStamp" text).replaceAll("(\\d{4}-\\d{2}-\\d{2})T(\\d{2}:\\d{2}:\\d{2})\\.\\d+", "$1 $2")  // timestamp

      (x \ "v").map{ x =>
        val ret = ArrayBuffer[String]()
        val strArr = (x text).split(" ", -1)
        if(strArr.length == 31){
          ret += List[String](strArr(10), strArr(11), strArr(0), strArr(2), strArr(4), strArr(5), strArr(6), strArr(7), strArr(23), strArr(24), strArr(25),
            strArr(26), strArr(27), "", strArr(29), strArr(30), strArr(13), strArr(14), strArr(1), strArr(3), strArr(21), "", "", "", "", "", "").mkString("|")
        }else{
          "-1"
        }
        ret += (x text).replace(" ", "|").replace("NIL", "")  // v
        ret += (eNBId, columnsValue, timeValue)
        ret.mkString("|")
      }.mkString("\n")
    }.mkString("\n")

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


    val result = sc.newAPIHadoopFile[LongWritable, Text, XmlInputFormat](srcPath).mapPartitions({x => x.map(y => parseXML1(y._2.toString))}).saveAsTextFile(tarPath)
  }
}
