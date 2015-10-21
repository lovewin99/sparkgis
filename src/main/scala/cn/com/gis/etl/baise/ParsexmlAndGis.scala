package cn.com.gis.etl.baise

/**
 * Created by wangxy on 15-10-20.
 */

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

import cn.com.utils.XmlInputFormat
import cn.com.gis.etl.baise.function.Process1

object ParsexmlAndGis {
  def parseXML1(content: String): Seq[Seq[String]] ={
    val a = xml.XML.loadString(content)
    // eNBId
    val eNBId = (a \ "eNB" \ "@id" text).toString

    val columns = List("@id", "@MmeUeS1apId", "@MmeCode", "@MmeGroupId")
    val m33 = "MR.LteScEarfcn MR.LteScPci MR.LteScRSRP MR.LteScRSRQ MR.LteScTadv MR.LteScPHR MR.LteScAOA MR.LteScSinrUL MR.LteScRI1 MR.LteScRI2 MR.LteScRI4 MR.LteScRI8 MR.LteSceNBRxTxTimeDiff MR.LteScBSR MR.LteScPUSCHPRBNum MR.LteScPDSCHPRBNum MR.LteNcEarfcn MR.LteNcPci MR.LteNcRSRP MR.LteNcRSRQ MR.TdsNcellUarfcn MR.TdsCellParameterId MR.TdsPccpchRSCP MR.GsmNcellNcc MR.GsmNcellBcc MR.GsmNcellBcch MR.GsmNcellCarrierRSSI"
    //    val m24 = "MR.LteScPlrULQci1 MR.LteScPlrULQci2 MR.LteScPlrULQci3 MR.LteScPlrULQci4 MR.LteScPlrULQci5 MR.LteScPlrULQci6 MR.LteScPlrULQci7 MR.LteScPlrULQci8 MR.LteScPlrULQci9 MR.LteScPlrDLQci1 MR.LteScPlrDLQci2 MR.LteScPlrDLQci3 MR.LteScPlrDLQci4 MR.LteScPlrDLQci5 MR.LteScPlrDLQci6 MR.LteScPlrDLQci7 MR.LteScPlrDLQci8 MR.LteScPlrDLQci9"
    //    val m7 = "MR.LteScRIP"

    // <measurement>...</measurement>
    // and smr == m33
    val c = (a \\ "measurement").filter(x => (x \ "smr" text) == m33) \\ "object"
    val result =c.map{ x =>
      val columnsValue = columns.map(x \ _ text).mkString("|")  // id MmeUeS1apId MmeCode MmeGroupId MmeGroupId TimeStamp
      val timeValue = (x \ "@TimeStamp" text).replaceAll("(\\d{4}-\\d{2}-\\d{2})T(\\d{2}:\\d{2}:\\d{2})\\.\\d+", "$1 $2")  // timestamp

      (x \ "v").map{ x =>
        val ret = ArrayBuffer[String]()
        ret += (x text).replace(" ", "|").replace("NIL", "")  // v
        ret += (eNBId, columnsValue, timeValue)
        ret.mkString("|")
//        ("1",ret.mkString("|"))
      }//.mkString("\n")
    }//.mkString("\n")
    result
  }

  def main(args: Array[String]):Unit={
    if (args.length != 2) {
      System.err.println("Usage: <in-file> <out-file>")
      System.exit(1)
    }

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
//    val Array(srcPath, tarPath) = args

    val xmap = Process1.Setup
    val x = sc.broadcast(xmap._1)
    val y = sc.broadcast(xmap._2)

    sc.hadoopConfiguration.set("xmlinput.start", "<bulkPmMrDataFile>")
    sc.hadoopConfiguration.set("xmlinput.end", "</bulkPmMrDataFile>")

  //  val result2 = sc.textFile(args(0))

    val result = sc.newAPIHadoopFile[LongWritable, Text, XmlInputFormat](args(0)).mapPartitions({x => x.map(y => parseXML1(y._2.toString))})
//    result.saveAsTextFile(args(2))
    val result1 = result.flatMap(_.flatMap(x=>x)).map(Process1.mapProcess).groupByKey().map(e => {Process1.reduceProcess(e._1, e._2, x.value, y.value)})
//    val result1 = result.mapPartitions(Iter => {
//      Iter.map(Process1.mapProcess)
//    }).groupByKey().map(e => {println(e._1);Process1.reduceProcess(e._1, e._2, x.value, y.value)})


    result1.saveAsTextFile(args(1))
  }
}
