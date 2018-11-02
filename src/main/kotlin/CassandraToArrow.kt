package CassandraToArrow
import MySQLToArrow.DataType
import com.datastax.driver.core.*
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.complex.ListVector
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.ArrayList


fun main(args: Array<String>) {
    val builder = Cluster.builder().addContactPoint("127.0.0.1")

    builder.withCredentials("user","user")
    builder.withPort(9042)

    val cluster = builder.build()
    val session = cluster.connect()

    /*
 *  Numeric Time Series with 1 column
 */

    val numericSequenceDTO = mutableMapOf<String, DataType>()
    numericSequenceDTO.put("Time", DataType.TimestampValue)
    numericSequenceDTO.put("Temperature", DataType.NumericalValue)


    val numericQueries = mutableMapOf<String, String>()
    numericQueries.put("Time", "select time from timeseries.temperatures where timeseries_name='Castle' and column_name='Temperature(F)' order by column_name asc, time asc limit 10;")
    numericQueries.put("Temperature" , "select value from timeseries.temperatures where timeseries_name='Castle' and column_name='Temperature(F)' order by column_name asc, time asc limit 10;")

    val numericVectors = mutableMapOf<String, ValueVector>()

    numericQueries.forEach()
    {
        val vector = when (numericSequenceDTO[it.key]) {
            is DataType.NumericalValue ->  cassandraToArrow(session, it.value, DataType.NumericalValue )
            is DataType.SymbolicValue  ->  cassandraToArrow(session, it.value, DataType.SymbolicValue )
            is DataType.TimestampValue ->  cassandraToArrow(session, it.value, DataType.TimestampValue )
            is DataType.UncertainValue ->  cassandraToArrow(session, it.value, DataType.UncertainValue )
            else -> cassandraToArrow(session, it.value, DataType.NumericalValue )
        }
        numericVectors.put(it.key, vector)
    }

    numericSequenceDTO.forEach{
        val values = arrowVectorToRaw(numericVectors, it.key, it.value)
        println(it.key+": ")
        values!!.forEach { println(it) }
    }


    val symbolicSequenceDTO = mutableMapOf<String, DataType>()
    symbolicSequenceDTO.put("Position", DataType.NumericalValue)
    symbolicSequenceDTO.put("Sample", DataType.SymbolicValue)

    val symbolicQueries = mutableMapOf<String, String>()
    symbolicQueries.put("Position", "select time from timeseries.dna where timeseries_name='Human' and column_name='Sample' order by column_name asc, time asc limit 10;")
    symbolicQueries.put("Sample" , "select value from timeseries.dna where timeseries_name='Human' and column_name='Sample' order by column_name asc, time asc limit 10;" )

    val symbolicVectors = mutableMapOf<String,ValueVector>()

    symbolicQueries.forEach()
    {
        val vector = when (symbolicSequenceDTO[it.key]) {
            is DataType.NumericalValue ->  cassandraToArrow(session, it.value, DataType.NumericalValue )
            is DataType.SymbolicValue  ->  cassandraToArrow(session, it.value, DataType.SymbolicValue )
            is DataType.TimestampValue ->  cassandraToArrow(session, it.value, DataType.TimestampValue )
            is DataType.UncertainValue ->  cassandraToArrow(session, it.value, DataType.UncertainValue )
            else -> cassandraToArrow(session, it.value, DataType.NumericalValue )
        }
        symbolicVectors.put(it.key, vector)
    }

    symbolicSequenceDTO.forEach{
        val values = arrowVectorToRaw(symbolicVectors, it.key, it.value)
        println(it.key+": ")
        values!!.forEach { println(it) }
    }




    val uncertainSequenceDTO = mutableMapOf<String, DataType>()
    uncertainSequenceDTO.put("Position", DataType.TimestampValue)
    uncertainSequenceDTO.put("Sample", DataType.UncertainValue)

    val uncertainQueries = mutableMapOf<String, String>()
    uncertainQueries.put("Position", "select time from timeseries.e_e_u where timeseries_name='TimeSeries0' and column_name='temperature' order by column_name asc, time asc limit 10;")
    uncertainQueries.put("Sample" , "select values from timeseries.e_e_u where timeseries_name='TimeSeries0' and column_name='temperature' order by column_name asc, time asc limit 10;" )

    val uncertainVectors = mutableMapOf<String,ValueVector>()

    uncertainQueries.forEach()
    {
        val vector = when (uncertainSequenceDTO[it.key]) {
            is DataType.NumericalValue ->  cassandraToArrow(session, it.value, DataType.NumericalValue )
            is DataType.SymbolicValue  ->  cassandraToArrow(session, it.value, DataType.SymbolicValue )
            is DataType.TimestampValue ->  cassandraToArrow(session, it.value, DataType.TimestampValue )
            is DataType.UncertainValue ->  cassandraToArrow(session, it.value, DataType.UncertainValue )
            else -> cassandraToArrow(session, it.value, DataType.NumericalValue )
        }
        uncertainVectors.put(it.key, vector)
    }

    uncertainSequenceDTO.forEach{
        val values = arrowVectorToRaw(uncertainVectors, it.key, it.value)
        println(it.key+": ")
        values!!.forEach { println(it) }
    }



    val numFromStringSequenceDTO = mutableMapOf<String, DataType>()
    numFromStringSequenceDTO.put("Time", DataType.TimestampValueWithFormat(SimpleDateFormat("yyyy-MM-dd")))
    numFromStringSequenceDTO.put("Temperature", DataType.NumericalValueFromString)


    val numFromStringQueries = mutableMapOf<String, String>()
    numFromStringQueries.put("Time", "select time from timeseries.temperatures_str where timeseries_name='Castle' and column_name='Temperature' order by column_name asc, time asc limit 10;")
    numFromStringQueries.put("Temperature" , "select value from timeseries.temperatures_str where timeseries_name='Castle' and column_name='Temperature' order by column_name asc, time asc limit 10;")

    val numFromStringVectors = mutableMapOf<String, ValueVector>()

    numFromStringQueries.forEach()
    {
        val vector = when (numFromStringSequenceDTO[it.key]) {
            is DataType.NumericalValue ->  cassandraToArrow(session, it.value, DataType.NumericalValue )
            is DataType.SymbolicValue  ->  cassandraToArrow(session, it.value, DataType.SymbolicValue )
            is DataType.TimestampValue ->  cassandraToArrow(session, it.value, DataType.TimestampValue )
            is DataType.UncertainValue ->  cassandraToArrow(session, it.value, DataType.UncertainValue )
            is DataType.NumericalValueFromString ->  cassandraToArrow(session, it.value, DataType.NumericalValueFromString )
            is DataType.TimestampValueWithFormat ->  cassandraToArrow(session, it.value, DataType.TimestampValueWithFormat(SimpleDateFormat("yyyy-MM-dd")) )
            else -> cassandraToArrow(session, it.value, DataType.NumericalValue )
        }
        numFromStringVectors.put(it.key, vector)
    }

    numFromStringSequenceDTO.forEach{
        val values = arrowVectorToRaw(numFromStringVectors, it.key, it.value)
        println(it.key+": ")
        values!!.forEach { println(it) }
    }

    session.close()
    cluster.close()
} //////////////////////////////////////MAIN/////////////////////////////////////////


fun cassandraToArrow(session: Session, query : String, dataType: DataType.NumericalValue) : Float8Vector
{
    val resultSet = session.execute(query)
    val vector = Float8Vector("", RootAllocator(Long.MAX_VALUE))
    //resultSet.toList() is required as Cassandra's ResultSet does not store the total size, but the number of rows retrieved (which may not be the same)
    val resultList = resultSet.toList()
    vector.setInitialCapacity(resultList.size)
    vector.allocateNew()
    var i = 0
    resultList.forEach{
        vector.setSafe(i, it.getDouble(0))
        i+=1
    }
    vector.valueCount = i
    return vector
}

fun cassandraToArrow(session: Session, query : String, dataType: DataType.NumericalValueFromString) : Float8Vector
{
    val resultSet = session.execute(query)
    val vector = Float8Vector("", RootAllocator(Long.MAX_VALUE))
    val resultList = resultSet.toList()
    vector.setInitialCapacity(resultList.size)
    vector.allocateNew()
    var i = 0
    resultList.forEach{
        vector.setSafe(i, it.getString(0).toDouble())
        i+=1
    }
    vector.valueCount = i
    return vector
}

fun cassandraToArrow(session: Session, query : String, dataType: DataType.SymbolicValue) : VarCharVector
{
    val resultSet = session.execute(query)
    val vector = VarCharVector("", RootAllocator(Long.MAX_VALUE))
    val resultList = resultSet.toList()
    vector.setInitialCapacity(resultList.size)
    vector.allocateNew()
    var i = 0
    resultList.forEach{
        vector.setSafe(i, it.getString(0).toByteArray())
        i+=1
    }
    vector.valueCount = i
    return vector
}

fun cassandraToArrow(session: Session, query : String, dataType: DataType.TimestampValue) : TimeStampVector
{
    val resultSet = session.execute(query)
    val vector = TimeStampNanoVector("", RootAllocator(Long.MAX_VALUE))
    val resultList = resultSet.toList()
    vector.setInitialCapacity(resultList.size)
    vector.allocateNew()
    var i = 0
    resultList.forEach{
        vector.setSafe(i, it.getTimestamp(0).time)
        i += 1
    }
    vector.valueCount = i
    return vector
}


//TODO: Not tested
fun cassandraToArrow(session: Session, query : String, dataType: DataType.TimestampValueSecondsFromString) : TimeStampVector
{
    val resultSet = session.execute(query)
    val vector = TimeStampNanoVector("", RootAllocator(Long.MAX_VALUE))
    val resultList = resultSet.toList()
    vector.setInitialCapacity(resultList.size)
    vector.allocateNew()
    var i = 0
    resultList.forEach{
        val dateString = it.getString(0)
        vector.setSafe(i, dateString.toLong())
        i += 1
    }
    vector.valueCount = i
    return vector
}


//TODO: Not tested
fun cassandraToArrow(session: Session, query : String, dataType: DataType.TimestampValueMillisFromString) : TimeStampVector
{
    val resultSet = session.execute(query)
    val vector = TimeStampNanoVector("", RootAllocator(Long.MAX_VALUE))
    val resultList = resultSet.toList()
    vector.setInitialCapacity(resultList.size)
    vector.allocateNew()
    var i = 0
    resultList.forEach{
        val dateString = it.getString(0)
        vector.setSafe(i, dateString.toLong())
        i += 1
    }
    vector.valueCount = i
    return vector
}


fun cassandraToArrow(session: Session, query : String, dataType: DataType.TimestampValueWithFormat) : TimeStampVector
{
    val resultSet = session.execute(query)
    val vector = TimeStampNanoVector("", RootAllocator(Long.MAX_VALUE))
    val resultList = resultSet.toList()
    vector.setInitialCapacity(resultList.size)
    vector.allocateNew()
    val format = dataType.simpleDateFormat
    var i = 0
    resultList.forEach{
        val dateString = it.getString(0)
        //FIXME: Throws exception
        vector.setSafe(i, format.parse(dateString).time)
        i += 1
    }
    vector.valueCount = i
    return vector
}

fun cassandraToArrow(session: Session, query : String, dataType: DataType.UncertainValue) : ListVector
{
    val resultSet = session.execute(query)
    val vector = ListVector.empty("", RootAllocator(Long.MAX_VALUE))
    val resultList = resultSet.toList()
    vector.setInitialCapacity(resultList.size)
    vector.allocateNew()
    val writer = vector.writer
    writer.allocate()
    var i = 0
    resultList.forEach{ row ->
        val list = row.getList(0, java.lang.Double::class.java)
        writer.position = i
        i += 1
        writer.startList()
        list.forEach {
            writer.float8().writeFloat8(it.toDouble())
        }
        writer.endList()
    }
    writer.setValueCount(i)
    vector.valueCount = i
    return vector
}


fun arrowVectorToRaw(vectors : MutableMap<String,ValueVector>, name: String, type: DataType) : List<Any>?{
    when (type) {
        is DataType.NumericalValue -> {
            val rawData = arrayListOf<Double>()
            val vector = vectors[name] as Float8Vector
            for (i in 0 until vector.valueCount) {
                rawData.add(vector.get(i))
            }
            return rawData
        }

        is DataType.NumericalValueFromString -> {
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

        is DataType.TimestampValueWithFormat -> {
            val rawData = arrayListOf<Timestamp>()
            val vector = vectors[name] as TimeStampVector
            for (i in 0 until vector.valueCount) {
                rawData.add(Timestamp(vector.get(i)))
            }
            return rawData
        }

        is DataType.TimestampValueSecondsFromString -> {
            val rawData = arrayListOf<Timestamp>()
            val vector = vectors[name] as TimeStampVector
            for (i in 0 until vector.valueCount) {
                rawData.add(Timestamp(vector.get(i)))
            }
            return rawData
        }

        is DataType.TimestampValueMillisFromString -> {
            val rawData = arrayListOf<Timestamp>()
            val vector = vectors[name] as TimeStampVector
            for (i in 0 until vector.valueCount) {
                rawData.add(Timestamp(vector.get(i)))
            }
            return rawData
        }

        is DataType.UncertainValue -> {
            val rawData = arrayListOf<ArrayList<Double>>()
            val vector = vectors[name] as ListVector
            for (i in 0 until vector.valueCount) {
                rawData.add(i, vector.getObject(i) as ArrayList<Double>)
            }
            return rawData
        }

        is DataType.SymbolicValue -> {
            val rawData = arrayListOf<String>()
            val vector = vectors[name] as VarCharVector
            for (i in 0 until vector.valueCount) {
                rawData.add(String(vector.get(i)))
            }
            return rawData
        }

        //TODO: Complete other DataTypes


    }
    return null
}