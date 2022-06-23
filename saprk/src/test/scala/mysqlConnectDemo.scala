import java.sql.DriverManager

object mysqlConnectDemo {
  def main(args: Array[String]): Unit = {
    val cont = DriverManager.getConnection("jdbc:mysql://localhost:3306/test/", "root", "123456")
    val stmt = cont.prepareStatement(s"select * from ?")
    stmt.setString(1, "table_name")
    val rst = stmt.executeQuery()
    rst.next()
    while (rst.next()) {
      val str = rst.getString(1)
    }
    try {
      cont.close()
      stmt.close()
      rst.close()
    } catch {
      case e: Exception => {
        println(e)
      }
    }
  }
}
