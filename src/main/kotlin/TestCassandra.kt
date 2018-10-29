import com.datastax.driver.core.exceptions.InvalidQueryException
import kotlin.system.exitProcess

//sealed class Types{
//    class MyInt() : Types()
//    class MyString() : Types()
//}
//
//data class SequenceSpec<T>(val value : T)
//
//class SequenceCollection(){
//    val col = mutableMapOf<String,Map<SequenceSpec<*>, Types>>()
//
//    fun insert(name : String, sequence : SequenceSpec<Int>){
//        val toStore = mutableMapOf<SequenceSpec<*>, Types>()
//        toStore[sequence] = Types.MyInt()
//        col[name] = toStore
//    }
//
//    fun insert(name : String, sequence : SequenceSpec<String>){
//        val toStore = mutableMapOf<SequenceSpec<*>, Types>()
//        toStore[sequence] = Types.MyString()
//        col[name] = toStore
//    }
//
//    fun retrieve(name: String) : SequenceSpec<*>?
//    {
//        val stored = col[name]
//        val seq = stored!!.keys.first()
//        val type = stored[seq]
//        when (type)  {
//            is Types.MyInt -> {
//                return seq as SequenceSpec<Int>
//            }
//            is Types.MyInt -> {
//                return seq as SequenceSpec<String>
//            }
//        }
//        return null
//    }
//}
//
//fun isInt(seq : SequenceSpec<Int>): Boolean
//{
//    return true
//}
//
//
//fun main(args: Array<String>) {
//    val a = SequenceSpec(1)
//    val b = SequenceSpec("hello")
//
//    isInt(a)
//    isInt(b)
//
//    val c = SequenceCollection()
//    c.insert("int",a)
//    c.insert("string",b)
//
//    val d = c.retrieve("int")
//    val e = c.retrieve("string")
//
//    isInt(d)
//    isInt(e)
//
//
//    println(a::class.java)
//    println(b::class.java)
//}

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
//        conn.insertMilkData()
        conn.selectSymbolic()
//        conn.insertTempData()
    }catch (e: InvalidQueryException)
    {
        println("Can't operate on table")
        e.printStackTrace()
        conn.close()
        exitProcess(-1)
    }

    conn.close()
}
