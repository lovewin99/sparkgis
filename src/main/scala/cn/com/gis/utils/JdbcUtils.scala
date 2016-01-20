package cn.com.gis.utils

import com.utils.ConfigUtils
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.Properties

/**
 * Created by wangxy on 16-1-5.
 */
object JdbcUtils {
  val propFile = "/config/dbinfo.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val odriver = prop.getOrElse("ojdbc.driver", "")
  val ourl = prop.getOrElse("ojdbc.url", "")
  val ouser = prop.getOrElse("ojdbc.username", "")
  val opassword = prop.getOrElse("ojdbc.password", "")


  def getOracleConnection(): Connection = {
    try {
      Class.forName(odriver)
      val connection = DriverManager.getConnection(ourl, ouser, opassword)
      connection
    }
    catch {
      case e: Throwable => {
        println("get oracle connection error!!!!!!!!!!!!!!!!")
        e.printStackTrace
        null
      }
    }
  }

  def ExecuteOSQL(sqlArr: Array[String]): Unit = {
    var stmt: Statement = null
    var conn: Connection = null

    try{
      conn = getOracleConnection()
      stmt = conn.createStatement()
      sqlArr.foreach{osql =>
        stmt.executeUpdate(osql)
      }
    }catch{
      case e: Throwable =>{
        println("ExecuteOSQL error!!!!")
        e.printStackTrace
      }
    }finally {
      if(stmt != null) stmt.close()
      if(conn != null) conn.close()
    }
  }

  def main(args: Array[String]) {
    val sqls = Array("INSERT INTO wxy VALUES ('v2')","INSERT INTO wxy VALUES ('v1')" )
    ExecuteOSQL(sqls)
  }

}
