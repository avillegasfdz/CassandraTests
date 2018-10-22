import com.datastax.driver.core.exceptions.InvalidQueryException
import kotlin.system.exitProcess

fun main(args: Array<String>) {

    var conn : MYSQLConnection
    try
    {
         conn = MYSQLConnection()

    }
    catch (e: Exception)
    {
        println("Can't connect to MYSQL")
        e.printStackTrace()
        exitProcess(-1)
    }
    println("Connected to MYSQL")

    conn.select_e_e_num_pts()


    conn.close()
    println("Connection closed")
}
