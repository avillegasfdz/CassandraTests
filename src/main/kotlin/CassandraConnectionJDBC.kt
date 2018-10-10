import com.datastax.driver.core.*
import com.datastax.driver.core.utils.Bytes
import java.io.File
import org.apache.cassandra.utils.UUIDGen
import java.sql.*


class CassandraConnectionJDBC(node: String, port: Int)
{

    val builder = Cluster.builder().addContactPoint(node)
    init {
        builder.withPort(port)
        builder.withCredentials("paco","pepe")
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

    fun insertData(){

        val prepared = session.prepare("""
            |                   INSERT INTO timeseries.raw_data(sensor,ts,readf)
            |                   VALUES (?, ?, ?);
                                """.trimMargin())
        val value = 1.0f
        for (i in 1..10) {

            session.execute(BoundStatement(prepared).bind("temperature", UUIDGen.getTimeUUID(), value*i))
        }

    }


    fun selectAll(keyspaceName: String, tableName: String)
    {
        //com.datastax.driver.core.exceptions.SyntaxError: Bind variables cannot be used for table names
        val queryString = "SELECT * FROM ${keyspaceName}.${tableName}"
        val resultSet = session.execute(queryString)

        resultSet.forEach()
        {
            println(it.toString())
        }
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