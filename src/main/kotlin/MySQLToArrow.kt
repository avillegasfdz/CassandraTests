package MySQLToArrow

import org.apache.arrow.vector.FieldVector
import org.apache.arrow.adapter.jdbc.JdbcToArrow
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.TimeStampVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.complex.ListVector

import java.io.Serializable
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.*

sealed class DataType : Serializable {
    /**
     * Value which whill be converted to Double.
     */
    object NumericalValue : DataType()
    /**
     * Value which whill be converted to Double.
     */
    object NumericalValueFromString : DataType()
    /**
     * Value which whill be converted to String.
     */
    object SymbolicValue : DataType()
    /**
     * Value which whill be converted to an Array of Double.
     */

    object UncertainValue : DataType()
    /**
     * Value which whill be converted to timestamp.
     */
    object TimestampValue : DataType()
    /**
     * Value which whill be converted to timestamp.
     */
    object TimestampValueSecondsFromString : DataType()
    /**
     * Value which whill be converted to timestamp.
     */
    object TimestampValueMillisFromString : DataType()
    /**
     * Value which whill be converted to timestamp.
     */
    data class TimestampValueWithFormat(val simpleDateFormat: SimpleDateFormat) : DataType()
}

data class Sequence(val sequenceDTO : MutableMap<String, DataType>, val queries : MutableMap<String, String>,
                    val vectors : MutableMap<String, FieldVector>)

fun connect() : Connection{
    val connectionProps = Properties()

    connectionProps.put("user", "user")
    connectionProps.put("password", "user")
    Class.forName("com.mysql.cj.jdbc.Driver").newInstance()


    var conn = try {
        DriverManager.getConnection(
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

    return conn
}


fun vectorToRaw(vectors : MutableMap<String,FieldVector>, name: String, type: DataType) : List<Any>?{
    when (type) {
        is DataType.NumericalValue -> {
            val rawData = arrayListOf<Double>()
            val vector = vectors[name] as Float8Vector
            for (i in 0 until vector.valueCount) {
                rawData.add(vector.get(i))
            }
            return rawData
        }

        is DataType.TimestampValue -> {
            val rawData = arrayListOf<Timestamp>()
            val vector = vectors[name] as TimeStampVector
            for (i in 0 until vector.valueCount) {
                rawData.add(Timestamp(vector.get(i)))
            }
            return rawData
        }

        //TODO: Implement
        is DataType.UncertainValue -> {
            val rawData = arrayListOf<ArrayList<Double>>()
            val vector = vectors[name] as ListVector
            for (i in 0 until vector.valueCount) {
                rawData.add(i, vector.getObject(i) as ArrayList<Double>)
            }
            return rawData
        }

        //TODO: Implement
        is DataType.SymbolicValue -> {
            val rawData = arrayListOf<String>()
            val vector = vectors[name] as VarCharVector
            for (i in 0 until vector.valueCount) {
                rawData.add(String(vector.get(i)))
            }
            return rawData
        }
    }
    return null
}

fun VarCharToListVector(vector : VarCharVector) : ListVector
{
    //TODO: Check name
    val result = ListVector.empty("",RootAllocator((Long.MAX_VALUE)) )
    result.setInitialCapacity(vector.valueCount)
    result.allocateNew()
    for (i in 0 until vector.valueCount) {
        var uncerts = vector.get(i).toString()
        uncerts = uncerts.drop(1)
        uncerts = uncerts.dropLast(1)
        val uncertsAsStringArray = uncerts.split(", ")
        val uncertsAsDoubleArray = uncertsAsStringArray.map { it.toDouble() }
        val writer = result.writer
        writer.allocate()
    //TODO
        writer.setValueCount(size)
    }
    result.valueCount = vector.valueCount
    return result
}


fun main(args: Array<String>) {

    val conn = connect()

    /*
     *  Numeric Time Series with 2 columns
     */

    val numericSequenceDTO = mutableMapOf<String, DataType>()
    numericSequenceDTO.put("Time", DataType.TimestampValue)
    numericSequenceDTO.put("Temperature", DataType.NumericalValue)
    numericSequenceDTO.put("Humidity", DataType.NumericalValue)

    val numericQueries = mutableMapOf<String, String>()
    numericQueries.put("Time", "select distinct time from timeseries.e_e_num order by time asc ;")
    numericQueries.put("Temperature" , "select value from timeseries.e_e_num where column_name='temperature' order by column_name asc, time asc ;" )
    numericQueries.put("Humidity" , "select value from timeseries.e_e_num where column_name='humidity' order by column_name asc, time asc ;" )

    val numericVectors = mutableMapOf<String,FieldVector>()


    numericQueries.forEach() {
        val vectorRoot = JdbcToArrow.sqlToArrow(conn, it.value, RootAllocator((Long.MAX_VALUE)))
        val vector = vectorRoot.fieldVectors[0]
        numericVectors.put(it.key, vector)
    }

    val numericSequence = Sequence(numericSequenceDTO, numericQueries, numericVectors)

    numericSequence.sequenceDTO.forEach(){
        val values = vectorToRaw(numericSequence.vectors,it.key,it.value)
        println(it.key+": ")
        values!!.forEach { println(it) }
    }

    /*
     *  Symbolic Time Series with 1 column
     */

    val symbolicSequenceDTO = mutableMapOf<String, DataType>()
    symbolicSequenceDTO.put("Position", DataType.NumericalValue)
    symbolicSequenceDTO.put("Sample", DataType.SymbolicValue)

    val symbolicQueries = mutableMapOf<String, String>()
    symbolicQueries.put("Position", "select distinct time from timeseries.e_e_sym order by time asc ;")
    symbolicQueries.put("Sample" , "select value from timeseries.e_e_sym where column_name='Sample' order by column_name asc, time asc ;" )

    val symbolicVectors = mutableMapOf<String,FieldVector>()


    symbolicQueries.forEach() {
        val vectorRoot = JdbcToArrow.sqlToArrow(conn, it.value, RootAllocator((Long.MAX_VALUE)))
        val vector = vectorRoot.fieldVectors[0]
        symbolicVectors.put(it.key, vector)
    }

    val symbolicSequence = Sequence(symbolicSequenceDTO,symbolicQueries,symbolicVectors)

    symbolicSequence.sequenceDTO.forEach(){
        val values = vectorToRaw(symbolicSequence.vectors,it.key,it.value)
        println(it.key+": ")
        values!!.forEach { println(it) }
    }

    /*
    *  Uncertain Time Series with 2 columns
    */

    val uncertainSequenceDTO = mutableMapOf<String, DataType>()
    uncertainSequenceDTO.put("Timestamp", DataType.TimestampValue)
    uncertainSequenceDTO.put("Temperature", DataType.UncertainValue)
    uncertainSequenceDTO.put("Humidity", DataType.UncertainValue)

    val uncertainQueries = mutableMapOf<String, String>()
    uncertainQueries.put("Timestamp", "select distinct time from timeseries.e_e_uncert order by time asc ;")
    uncertainQueries.put("Temperature" , "select value from timeseries.e_e_uncert where column_name='temperature' order by column_name asc, time asc ;" )
    uncertainQueries.put("Humidity" , "select value from timeseries.e_e_uncert where column_name='humidity' order by column_name asc, time asc ;" )

    val uncertainVectors = mutableMapOf<String,FieldVector>()


    uncertainQueries.forEach() {
        val vectorRoot = JdbcToArrow.sqlToArrow(conn, it.value, RootAllocator((Long.MAX_VALUE)))
        val vector = vectorRoot.fieldVectors[0]
        if (uncertainSequenceDTO[it.key] is DataType.UncertainValue)
        {
            uncertainVectors.put(it.key, VarCharToListVector(vector))
        } else {
            uncertainVectors.put(it.key, vector)
        }

    }

    val uncertainSequence = Sequence(uncertainSequenceDTO,uncertainQueries, uncertainVectors)

    uncertainSequence.sequenceDTO.forEach(){
        val values = vectorToRaw(uncertainSequence.vectors,it.key,it.value)
        println(it.key+": ")
        values!!.forEach { println(it) }
    }

    conn.close()
}