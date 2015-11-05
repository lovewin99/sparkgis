package cn.com.gis.tools

/**
 * Created by wangxy on 15-11-5.
 */

//待处理数据
class XDR_UE_MR_S2 extends Serializable{
  var cell_id_ : Int = -1
  var ta_ : Int = -1
  var aoa_ : Int = -1
  var serving_freq_ : Int = -1
  var serving_rsrp_ : Int = -1
  var time_ : Long = -1

  // 以下为临区信息
//  var nei_cell_pci_ : Int = -1
//  var nei_freq_ : Long = -1L
//  var nei_rsrp_ : Long = -1L
  //  var nei_rsrq_ : Int = -1
}

//临时保存的用户信息
class User2 extends Serializable{
  var cell_id : Long = -1
  var ta_ : Int = -1
  var aoa_ : Int = -1
  var time : Long = -1
}

class StaticCellInfo2 extends Serializable{
  var enb_height_ : Double  = 40.0        // 挂高(m)
  var crs_power_ : Double = 7.2           //
  var ant_gain_ : Double = 14.5           // db
  var in_door_ : Int = -1                  // 室内小区

  var cellid_ : Int = -1                  // enbid<<8 |	cell_in_enb
  var longitude_ : Double = 0.0           // longitude
  var latitude_ : Double = 0.0            //	latitude
  var cell_pci_ : Long = -1L	              // cell_pci
  var freq_ : Long = -1L                    // freq
  var azimuth_ : Int = -1                  //方向角			azimuth
  var pci_freq : Long = -1L

}

object DataSt2 {

}
