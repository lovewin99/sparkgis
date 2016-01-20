package cn.com.gis.etl.location.function
import math._
/**
 * Created by wangxy on 16-1-5.
 * 根据tac eci or la ci 定位
 */
object TacEcifunction1 {
  def location(taceci: String, sgInfo: Map[String, String]): Array[String] = {
    sgInfo.get(taceci) match{
      case None => Array("", "")
      case Some(locations) => {
        val gisArr = locations.split(",", -1)
        val index = rint(random*(gisArr.length-1)).toInt
        gisArr(index).split(" ", -1)
      }
    }
  }
}
