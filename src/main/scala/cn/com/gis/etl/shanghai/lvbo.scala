package cn.com.gis.etl.shanghai

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by wangxy on 15-11-18.
 */
object lvbo {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      System.out.print("error input: <input-path> <output-path>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("gsmgis1 Application")
    val sc = new SparkContext(conf)

    val textRdd = sc.textFile(args(0)).filter(_ != "-1|-1")

    // 输入(lon|lat, x|y, distance, sampling, time)
    textRdd.map{
      case in: String => {
        val strArr = in.split(",")
      }
      case _ => "-1"
    }
  }
}
