import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import java.io.File


fun main(args: Array<String>) {

    val filePath = "./src/main/resources/example.arrow"

    //Create Fields
    val doubleField = Field("MyField", FieldType.nullable(ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)

    //Create schema
    val builder = mutableListOf<Field>()
    builder.add(doubleField)
    val writeSchema = Schema(builder, null)

    //Write to Arrow
    val fileToWrite = File(filePath)
    val writeStream = fileToWrite.outputStream()
    val writeRoot = VectorSchemaRoot.create(writeSchema, RootAllocator(Long.MAX_VALUE))
    val writer = ArrowFileWriter(writeRoot, DictionaryProvider.MapDictionaryProvider(), writeStream.channel)

    writer.start()

    writeRoot.rowCount = 100
    val vector = writeRoot.getVector("MyField") as Float8Vector
    vector.setInitialCapacity(100)
    vector.allocateNew()
    for (index in 0 until 100) {
        vector.set(index, index.toDouble())
        println("$index, ${index.toDouble()}")
    }
    vector.valueCount = 100
    writer.writeBatch()
    writer.end()
    writer.close()

    //Read from Arrow
    val fileToRead = File(filePath)
    val readStream = fileToRead.inputStream()
    val reader = ArrowFileReader(readStream.channel, RootAllocator(Long.MAX_VALUE))
    val readRoot = reader.vectorSchemaRoot
    val readSchema = readRoot.schema


    readSchema.fields.forEach { field ->

        println("Field name: ${field.name} type: ${field.type}")
        val arrowBlocks = reader.recordBlocks
        arrowBlocks.forEach { block ->
            reader.loadRecordBatch(block)
            val fieldVector = readRoot.getVector(field.name)
            val fileReader = fieldVector.reader
            val type = fieldVector.minorType
            when (type.name) //TODO: review this
            {
                "FLOAT8" -> {
                    for (i in 0 until fieldVector.valueCount) {
                        fileReader.position = i
                        println("$i, ${fileReader.readDouble()}")
                    }
                }
            }
        }
    }


    reader.close()


}