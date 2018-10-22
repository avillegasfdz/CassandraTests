import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*

class MYSQLConnection() {




    private val connectionProps = Properties()
    init {
        connectionProps.put("user", "user")
        connectionProps.put("password", "user")
        Class.forName("com.mysql.cj.jdbc.Driver").newInstance()
    }

    private var conn = try {DriverManager.getConnection(
                    "jdbc:" + "mysql" + "://" +
                            "127.0.0.1" +
                            ":" + "3306" + "/" +
                            "",
                    connectionProps)
        } catch (e: SQLException)
        {
            // handle any errors
            e.printStackTrace()
        } catch (e: Exception)
        {
            // handle any errors
            e.printStackTrace()
        } as Connection



    fun select_e_e_num_pts()
    {
        var query = "Select * from timeseries.e_e_num_nts order by column_name asc, time asc"
        val statement = conn.createStatement()
        val resultSet = statement.executeQuery(query)

        while (resultSet.next())
        {
            println(resultSet.getString(1) + " " + resultSet.getString("column_name") + " " +
                    resultSet.getDouble("time") + " " + resultSet.getDouble("value") )
        }
    }



    fun close()
    {
        conn.close()
    }
}