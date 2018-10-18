import com.datastax.driver.core.exceptions.InvalidQueryException
import kotlin.system.exitProcess

fun main(args: Array<String>) {

    var conn : CassandraConnection
    try
    {
         conn = CassandraConnection("127.0.0.1", 9042)
    }
    catch (e: Exception)
    {
        println("Can't connect to Cassandra")
        e.printStackTrace()
        exitProcess(-1)
    }
    println("Connected to Cassandra")

//    try {
//        conn.createKeyspace("timeseries", "SimpleStrategy", 1)
//        println("Keyspace created")
//    }catch (e: Exception)
//    {
//        println("Can't create keyspace")
//        e.printStackTrace()
//        conn.close()
//        exitProcess(-1)
//    }

//    try {
//        conn.createTable("timeseries", "raw_data")
//        println("Table created")
//
//    }catch (e: Exception)
//    {
//        println("Can't create table")
//        e.printStackTrace()
//        conn.close()
//        exitProcess(-1)
//    }

    try {
        //conn.cleanTable("library", "books")
//        conn.insertDataUnevenly()
//        conn.insertDataEvenlyImplicit()
//        conn.insertDataEvenlyExplicit()
//          conn.insertDataEvenlyExplicitSymbolic()
//            conn.insertDataEvenlyExplicitNumeric()
//        conn.insertDataEvenlyExplicitUncertain()
//        println("Unevenly")
//        conn.selectAll("timeseries", "e_e_u")
//        println("Evenly implicit")
//        conn.selectAll("timeseries", "e_e_u")
       // println("Evenly explicit")
        //conn.selectUncertainSymbolic("timeseries", "e_e_u_sym")
        //conn.insertDataEvenlyExplicitSymbolic()
//        conn.selectSymbolic("timeseries", "e_e_sym")
        conn.insertMilkData()
    }catch (e: InvalidQueryException)
    {
        println("Can't operate on table")
        e.printStackTrace()
        conn.close()
        exitProcess(-1)
    }

    conn.close()
}
