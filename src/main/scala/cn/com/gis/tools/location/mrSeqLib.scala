package cn.com.gis.tools.location

import com.utils.ConfigUtils

/**
 * Created by wangxy on 16-1-5.
 */
object mrSeqLib {
  val propFile = "/config/mrlocation.properties"
  val prop = ConfigUtils.getConfig(propFile)
  // 随机栅格表名
  val sgRname = prop.getOrElse("SG_RANDOM_NAME", "")

  // 字段顺序
}
