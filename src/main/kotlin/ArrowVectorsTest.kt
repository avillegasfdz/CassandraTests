package ArrowVectorsTest
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

fun rawToVector(vectors : MutableMap<String,FieldVector>, name: String, rawData: List<Any>, type: DataType, size: Int){

    when (type) {
        is DataType.NumericalValue -> {
            val vector = Float8Vector(name, RootAllocator(Long.MAX_VALUE))
            vector.setInitialCapacity(size)
            vector.allocateNew()
            for (index in 0 until size) {
                vector.setSafe(index, rawData[index] as Double)
            }
            vector.valueCount = size
            vectors.put(name,vector)
        }

        is DataType.TimestampValue -> {
            val fieldType = FieldType.nullable(ArrowType.Timestamp(TimeUnit.MILLISECOND, null))
            val vector = TimeStampMilliVector(name, fieldType, RootAllocator(Long.MAX_VALUE))
            vector.setInitialCapacity(size)
            vector.allocateNew()
            for (index in 0 until size) {
                val valueToInsert = rawData[index] as Timestamp
                vector.setSafe(index, valueToInsert.time)
            }
            vector.valueCount = size
            vectors.put(name,vector)
        }

        //TODO: Implement
        is DataType.UncertainValue -> {
            val vector = ListVector.empty(name,RootAllocator((Long.MAX_VALUE)) )
            vector.setInitialCapacity(size)
            vector.allocateNew()
            val writer = vector.writer
            writer.allocate()
            for (index in 0 until size) {
                val valueToInsert = rawData[index] as ArrayList<Double>
                writer.position = index
                writer.startList()
                valueToInsert.forEach(){
                    writer.float8().writeFloat8(it)
                }
                writer.endList()
            }
            writer.setValueCount(size)
            vector.valueCount = size
            vectors.put(name,vector)
        }


        is DataType.SymbolicValue -> {
            val vector = VarCharVector(name,RootAllocator((Long.MAX_VALUE)))
            vector.setInitialCapacity(size)
            vector.allocateNew()
            for (index in 0 until size) {
                val valueToInsert = rawData[index] as String
                //TODO: This is taken from the Arrow example, but to me it seems unsafe
                vector.setSafe(index, valueToInsert.toByteArray())
            }
            vector.valueCount = size
            vectors.put(name,vector)
        }
    }
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
    val uncertainValues = arrayListOf<Double>()
    for (i in 0 until NELEM)
    {
        uncertainValues.add(i, i.toDouble())
        uncertainData.add(i, uncertainValues.clone() as ArrayList<Double>)
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

    val vectors = mutableMapOf<String,FieldVector>()

    rawToVector(vectors, "Timestamp",timestampData, DataType.TimestampValue, NELEM)
    rawToVector(vectors, "Numeric",numericData, DataType.NumericalValue, NELEM)
    rawToVector(vectors, "Uncertain",uncertainData, DataType.UncertainValue, NELEM)
    rawToVector(vectors, "Symbolic",symbolicData, DataType.SymbolicValue, NELEM)


    val returnedTimestamp = vectorToRaw(vectors, "Timestamp", DataType.TimestampValue)
    val returnedNumeric = vectorToRaw(vectors, "Numeric", DataType.NumericalValue)
    val returnedSymbolic = vectorToRaw(vectors, "Symbolic", DataType.SymbolicValue)
    val returnedUncertain = vectorToRaw(vectors, "Uncertain", DataType.UncertainValue)


    returnedTimestamp?.forEach { println(it) }
    returnedNumeric?.forEach { println(it) }
    returnedSymbolic?.forEach { println(it) }
    returnedUncertain?.forEach{ println(it) }

}