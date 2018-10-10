import com.datastax.driver.core.*
import com.datastax.driver.core.utils.Bytes
import org.apache.cassandra.db.marshal.TimeUUIDType
import java.io.File
import org.apache.cassandra.utils.UUIDGen
import java.sql.Timestamp
import java.util.*
import kotlin.collections.ArrayList


class CassandraConnection(node: String, port: Int)
{

    val builder = Cluster.builder().addContactPoint(node)
        init {
            builder.withCredentials("user","user")
            builder.withPort(port)
        }
    val cluster = builder.build()
    val session = cluster.connect()


    fun close()
    {
        session.close()
        cluster.close()
    }

    fun createKeyspace(keyspaceName: String, replicationStrategy: String, replicationFactor: Int) {
        val queryString =   """ CREATE KEYSPACE IF NOT EXISTS ${keyspaceName}
                                WITH replication =
                                        {   'class':'${replicationStrategy}',
                                            'replication_factor':${replicationFactor}   };
                            """.trimMargin()
        session.execute(queryString)

    }

    fun createTable(keyspaceName: String, tableName: String){
        //com.datastax.driver.core.exceptions.SyntaxError: Bind variables cannot be used for table names
        val queryString = """ CREATE TABLE IF NOT EXISTS ${keyspaceName}.${tableName} (
                sensor text,
                ts timeuuid,
                readf float,
                primary key (sensor, ts)
               ) WITH CLUSTERING ORDER BY (ts DESC)
                    AND compaction = {  'class': 'TimeWindowCompactionStrategy',
                                        'compaction_window_size': 1,
                                        'compaction_window_unit': 'DAYS'};
        """.trimMargin()
        session.execute(queryString)
    }

    fun cleanTable(keyspaceName: String, tableName: String)
    {
        //com.datastax.driver.core.exceptions.SyntaxError: Bind variables cannot be used for table names
        session.execute("TRUNCATE TABLE ${keyspaceName}.${tableName};")
    }

    fun insertDataUnevenly(){

        val prepared = session.prepare("""
            |                   INSERT INTO timeseries.raw_data_unevenly(sensor,ts,value)
            |                   VALUES (?, ?, ?);
                                """.trimMargin())
        val value = 1.0f
        for (i in 1..10) {
            Thread.sleep(i.toLong())
            session.execute(BoundStatement(prepared).bind("temperature", UUIDGen.getTimeUUID(), value*i))
        }

    }

    fun insertDataEvenlyImplicit(){

        val prepared = session.prepare("""
            |                   INSERT INTO timeseries.raw_data_evenly_implicit(sensor,ts,value)
            |                   VALUES (?, ?, ?);
                                """.trimMargin())
        val value = 1.0f
        for (i in 1..10) {
            session.execute(BoundStatement(prepared).bind("temperature", UUIDGen.getTimeUUID(i.toLong()*1000), value*i))
        }
    }
    fun insertDataEvenlyExplicit(){

        val prepared = session.prepare("""
            |                   INSERT INTO timeseries.e_e_u(timeseries_name, column_name, time,values)
            |                   VALUES (?, ?, ?, ?);
                                """.trimMargin())
        val values = arrayListOf<Float>()
        val now = System.currentTimeMillis()

        for (i in 1..10) {
            values.add(i.toFloat())
            session.execute(BoundStatement(prepared).bind( "TimeSeries0",
                                                            "temperature",
                                                            Timestamp(now+i.toLong()*1000),
                                                            values))
            session.execute(BoundStatement(prepared).bind("TimeSeries0",
                    "humidity",
                    Timestamp(now+i.toLong()*1000),
                    values))

        }
    }

    fun selectAll(keyspaceName: String, tableName: String)
    {

        val values = mutableMapOf<String, ArrayList<Any>>()
        //Extact Time
        val rSetTime = session.execute("""select time from timeseries.e_e_u
               where timeseries_name='TimeSeries0' and column_name='temperature'
               order by column_name asc, time asc;""".trimMargin())
        val columnTime = arrayListOf<Any>()
        rSetTime.forEach {
            columnTime.add(UUIDGen.microsTimestamp(it.getUUID("ts")))
        }
        values["ts"] = columnTime

        //Extract Value
        val rSetSensor = session.execute("select sensor, value from timeseries.raw_data_evenly_explicit where sensor='temperature' order by ts asc;")
        val columnSensor = arrayListOf<Any>()
        rSetSensor.forEach {
            columnSensor.add(it.getList("value", Float.javaClass))
        }
        values["ts"] = columnTime
    }

    fun readData(keyspaceName: String, tableName: String)
    {
        val queryString = "SELECT * FROM ${keyspaceName}.${tableName}"
        val resultSet = session.execute(queryString)

        resultSet.forEach(){
            if(it.getBytes("file") != null)
            {
                val path = "./resources/${it.getUUID("id").toString()}.txt"
                File(path).writeBytes(Bytes.getArray(it.getBytes("file")))
                println("File ${path} created")
            }
        }
    }


}