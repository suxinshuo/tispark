package com.pingcap.tispark.utils

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}

object JdbcUtil {

  def getConn(url: String, user: String, password: String): Connection = {
    try {
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection(url, user, password)
    } catch {
      case e: SQLException =>
        throw new RuntimeException(e)
      case e: ClassNotFoundException =>
        throw new RuntimeException(e)
    }
  }

  def closeConnection(connection: Connection, pstmt: PreparedStatement): Unit = {
    try if (pstmt != null) pstmt.close()
    catch {
      case e: SQLException =>
        throw new RuntimeException(e)
    } finally if (connection != null) try connection.close()
    catch {
      case e: SQLException =>
        throw new RuntimeException(e)
    }
  }

}
