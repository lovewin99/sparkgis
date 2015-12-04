package cn.com.gis.tools

import scala.math._

/**
 * Created by wangxy on 15-11-30.
 */
object cooTranslate {

  // 中央经线
  val medien = 121.0
//  val x = 3420252.5173054
//  val y = 556413.619816685
  val x = 3420252.0
  val y = 556413.0

  // 划分栅格
  def gkp2sg(xsrc: Double, ysrc: Double): (Double, Double) = {
    val xdes = x + rint((xsrc - x) / 20)
    val ydes = y + rint((ysrc - y) / 20)
    (xdes, ydes)
  }

  def lonlat2gkp(lon_orig: Double, lat_orig: Double, meridian: Double): (Double, Double) = {
    val e2 = 0.006693421622966
    val ee2 = 0.006738525414683
    val a = 6378245
    val L = lon_orig
    val B0 = lat_orig
    val B = (lat_orig * 2 * Pi) / 360
    // B是弧度为单位
    // L B0都是以度为单位

    val X = 111134.8611*B0-16036.4803*sin(2*B)+16.8281*sin(4*B)-0.022*sin(6*B)
    val p = 3600*180 / Pi
    val W = sqrt(1-e2*pow(sin(B), 2))
    val N = a/W
    val t = tan(B)
    val yita =  ee2*cos(B)*cos(B)
    val l = L-meridian
    val ll = l*3600
    val x = X+(N*sin(B)*cos(B)*pow(ll,2))/(2*pow(p,2))+N*sin(B)*pow(cos(B),3)*(5-pow(t,2) + 9*yita + 4*pow(yita,2))*pow(ll,4)/(24*pow(p,4)) +
      N*sin(B)*pow(cos(B),5)*(61-58*pow(t,2) + pow(t,4))*pow(ll,6)/(720*pow(p,6))
    val y = N*cos(B)*ll/p+N*pow(cos(B),3)*(1-pow(t,2)+yita)*pow(ll,3)/(6*pow(p,3))+N*pow(cos(B),5)*(5-18*pow(t,2)+pow(t,4) + 14*yita -
      58*yita*pow(t,2))*pow(ll,5)/(120*pow(p,5)) + 500000
    (x, y)
  }

  def main(args: Array[String]): Unit = {
    val xy1 = lonlat2gkp(121.668458,30.885778,121.0)
    val xy = gkp2sg(xy1._1, xy1._2)
    println(s"x = ${xy._1}  y = ${xy._2}")
  }

}
