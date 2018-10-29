import com.google.common.collect.ImmutableList
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import java.io.File
import java.io.Serializable
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.*
import javax.xml.crypto.Data
import kotlin.collections.ArrayList

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
    // TODO Check the correct functionality of uncertain values.
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

fun rawToVector(root: VectorSchemaRoot, name: String, rawData: List<Any>, type: DataType, size: Int){

    when (type) {
        is DataType.NumericalValue -> {
            val vector = root.getVector(name) as Float8Vector
            vector.setInitialCapacity(size)
            vector.allocateNew()
            for (index in 0 until size) {
                vector.set(index, rawData[index] as Double)
            }
            vector.valueCount = size
        }

        is DataType.TimestampValue -> {
            val vector = root.getVector(name) as TimeStampVector
            vector.setInitialCapacity(size)
            vector.allocateNew()
            for (index in 0 until size) {
                val valueToInsert = rawData[index] as Timestamp
                vector.set(index, valueToInsert.time)
            }
            vector.valueCount = size
        }

        //TODO: Implement
        is DataType.UncertainValue -> {
            val vector = root.getVector(name) as ListVector
            vector.setInitialCapacity(size)
            vector.allocateNew()
//            for (index in 0 until size) {
//                val valueToInsert = rawData[index] as Timestamp
//                vector.set(index, valueToInsert.time)
//            }
            vector.valueCount = size
        }

        //TODO: Implement
        is DataType.SymbolicValue -> {
            val vector = root.getVector(name) as VarCharVector
            vector.setInitialCapacity(size)
            vector.allocateNew()
            for (index in 0 until size) {
                val valueToInsert = rawData[index] as String
                vector.set(index, valueToInsert.toByteArray())
            }
            vector.valueCount = size
        }
    }
}


fun vectorToRaw(root: VectorSchemaRoot, name: String, type: DataType) : List<Any>?{
    when (type) {
        is DataType.NumericalValue -> {
            val rawData = arrayListOf<Double>()
            val vector = root.getVector(name) as Float8Vector
            for (i in 0 until vector.valueCount) {
                rawData.add(vector.get(i))
            }
            return rawData
        }



        is DataType.TimestampValue -> {
            val rawData = arrayListOf<Timestamp>()
            val vector = root.getVector(name) as TimeStampVector
            for (i in 0 until vector.valueCount) {
                rawData.add(Timestamp(vector.get(i)))
            }
            return rawData
        }

        //TODO: Implement
        is DataType.UncertainValue -> {
            val rawData = arrayListOf<Timestamp>()
            val vector = root.getVector(name) as TimeStampVector
//            for (i in 0 until vector.valueCount) {
//                rawData.add(Timestamp(vector.get(i)))
//            }
            return rawData
        }

        //TODO: Implement
        is DataType.SymbolicValue -> {
            val rawData = arrayListOf<String>()
            val vector = root.getVector(name) as VarCharVector
            for (i in 0 until vector.valueCount) {
                rawData.add(String(vector.get(i)))
            }
            return rawData
        }
    }
    return null
}



fun main(args: Array<String>) {

    //Generate Data
    val NELEM = 8

    val numericData = arrayListOf<Double>()
    for (i in 1..NELEM)
    {
        numericData.add(i.toDouble())
    }

    val symbolicData = arrayListOf("A", "C", "G", "T", "A", "C", "G", "T")

    val uncertainData = arrayListOf<ArrayList<Double>>()
    val uncertainValues = arrayListOf(0.0)
    for (i in 1..NELEM)
    {
        uncertainData.add(uncertainValues)
        uncertainValues.add(i.toDouble())
    }

    val timestampData = arrayListOf<Timestamp>()
    var now = System.currentTimeMillis()
    for (i in 1..NELEM) {
        timestampData.add(Timestamp(now))
        now += 1000
    }

    //Simulate the TS
    val sequenceDTO = mutableMapOf<String, DataType>()
    sequenceDTO.put("Timestamp", DataType.TimestampValue)
    sequenceDTO.put("Numeric", DataType.TimestampValue)
    sequenceDTO.put("Uncertain", DataType.UncertainValue)
    sequenceDTO.put("Symbolic", DataType.SymbolicValue)

    //Create Fields
    val timestampField = Field("Timestamp", FieldType.nullable(ArrowType.Timestamp(TimeUnit.MILLISECOND, TimeZone.getDefault().toString())), null)
    val numericField = Field("Numeric", FieldType.nullable(ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
    val uncertainField = Field("Uncertain", FieldType.nullable(ArrowType.List()), null)
    val symbolicField = Field("Symbolic", FieldType.nullable(ArrowType.Utf8()), null)


    //Create schema
    val builder = ImmutableList.builder<Field>()//mutableListOf<Field>()
    builder.add(timestampField)
    builder.add(numericField)
//    builder.add(uncertainField)
    builder.add(symbolicField)
    val schema = Schema(builder.build(), null)


    //Store in Arrow schema
    val root = VectorSchemaRoot.create(schema, RootAllocator(Long.MAX_VALUE))

    rawToVector(root, "Timestamp",timestampData, DataType.TimestampValue, NELEM)
    rawToVector(root, "Numeric",numericData, DataType.NumericalValue, NELEM)
//    rawToVector(root, "Uncertain",uncertainData, DataType.UncertainValue, NELEM)
    rawToVector(root, "Symbolic",symbolicData, DataType.SymbolicValue, NELEM)


    val returnedTimestamp = vectorToRaw(root, "Timestamp", DataType.TimestampValue)
    val returnedNumeric = vectorToRaw(root, "Numeric", DataType.NumericalValue)
    val returnedSymbolic = vectorToRaw(root, "Symbolic", DataType.SymbolicValue)


    returnedTimestamp?.forEach { println(it) }
    returnedNumeric?.forEach { println(it) }
    returnedSymbolic?.forEach { println(it) }
}