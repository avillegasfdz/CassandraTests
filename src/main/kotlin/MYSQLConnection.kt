import java.io.BufferedReader
import java.io.FileReader
import java.sql.*
import java.text.SimpleDateFormat
import java.util.*

class MYSQLConnection() {




    private val connectionProps = Properties()
    init {
        connectionProps.put("user", "user")
        connectionProps.put("password", "user")
        Class.forName("com.mysql.cj.jdbc.Driver").newInstance()
    }

    private var conn = try {DriverManager.getConnection(
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



    fun select_e_e_num_pts()
    {
        var query = "Select * from timeseries.e_e_num_nts order by column_name asc, time asc"
        val statement = conn.createStatement()
        val resultSet = statement.executeQuery(query)

        while (resultSet.next())
        {
            println(resultSet.getString(1) + " " + resultSet.getString("column_name") + " " +
                    resultSet.getDouble("time") + " " + resultSet.getDouble("value") )
        }
    }


    fun insert_e_e_num()
    {
        val query = """INSERT INTO timeseries.e_e_num(timeseries_name, column_name, time, value)
            |          VALUES (?, ?, ?, ?); """.trimMargin()

        val statement = conn.prepareStatement(query)

        val now = System.currentTimeMillis()

        for (i in 1..10) {

            statement.setString(1, "TimeSeries0")
            statement.setString(2, "humidity")
            statement.setTimestamp(3, Timestamp(now+i.toLong()*1000))
            statement.setDouble(4, 1.0*i)
            statement.execute()


            statement.setString(1, "TimeSeries0")
            statement.setString(2, "temperature")
            statement.setTimestamp(3, Timestamp(now+i.toLong()*1000))
            statement.setDouble(4, 1.0*i)
            statement.execute()


        }
    }

    fun select_e_e_num()
    {
        var query = "Select * from timeseries.e_e_num order by column_name asc, time asc"
        val statement = conn.createStatement()
        val resultSet = statement.executeQuery(query)

        while (resultSet.next())
        {
            println(resultSet.getString(1) + " " + resultSet.getString("column_name") + " " +
                    resultSet.getTimestamp("time") + " " + resultSet.getDouble("value") )
        }
    }

    fun select_e_e_sym()
    {
        var query = "Select * from timeseries.e_e_sym order by column_name asc, time asc"
        val statement = conn.createStatement()
        val resultSet = statement.executeQuery(query)

        while (resultSet.next())
        {
            println(resultSet.getString(1) + " " + resultSet.getString("column_name") + " " +
                    resultSet.getDouble("time") + " " + resultSet.getString("value") )
        }
    }


    fun insert_e_e_uncert()
    {
        val query = """INSERT INTO timeseries.e_e_uncert(timeseries_name, column_name, time, value)
            |          VALUES (?, ?, ?, ?); """.trimMargin()

        val statement = conn.prepareStatement(query)

        val now = System.currentTimeMillis()
        val values = arrayListOf<Double>()
        for (i in 1..10) {
            values.add(i.toDouble())

            statement.setString(1, "TimeSeries0")
            statement.setString(2, "humidity")
            statement.setTimestamp(3, Timestamp(now+i.toLong()*1000))
            statement.setString(4, values.toString())
            statement.execute()


            statement.setString(1, "TimeSeries0")
            statement.setString(2, "temperature")
            statement.setTimestamp(3, Timestamp(now+i.toLong()*1000))
            statement.setString(4, values.toString())
            statement.execute()

        }
    }

    fun select_e_e_uncert()
    {
        var query = "Select * from timeseries.e_e_uncert order by column_name asc, time asc"
        val statement = conn.createStatement()
        val resultSet = statement.executeQuery(query)

        while (resultSet.next())
        {
            var uncerts = resultSet.getString("value")
            uncerts = uncerts.drop(1)
            uncerts = uncerts.dropLast(1)
            val uncertsAsStringArray = uncerts.split(", ")
            val uncertsAsDoubleArray = uncertsAsStringArray.map { it.toDouble() }
            println(resultSet.getString(1) + " " + resultSet.getString("column_name") + " " +
                    resultSet.getTimestamp("time") + " " + uncertsAsDoubleArray.toString() )
        }
    }

    fun insertTemperatureData()
    {
        val query = """INSERT INTO timeseries.temperatures (timeseries_name, column_name, time, value)
            |                               VALUES ('Temperature', 'Nottingham', ?, ?);""".trimMargin()

        val statement = conn.prepareStatement(query)

        var line: String?



        val fileReader = BufferedReader(FileReader("./src/main/resources/mean-monthly-air-temperature-deg.csv"))

        // Read CSV header
        fileReader.readLine()

        // Read the file line by line starting from the second line
        line = fileReader.readLine()
        while (line != null) {
            val tokens = line.split(";")
            if (tokens.size > 0) {
                val sdf = SimpleDateFormat("yyyy-MM")
                val date = sdf.parse(tokens[0].replace("\"",""))
                val ts = Timestamp(date.time)
                val v = tokens[1].toDouble()
                statement.setString(1, ts.toString())
                statement.setDouble(2,v)
                statement.execute()
            }

            line = fileReader.readLine()
        }

    }

    fun select_temperature()
    {
        var query = "Select * from timeseries.temperatures order by column_name asc, time asc"
        val statement = conn.createStatement()
        val resultSet = statement.executeQuery(query)

        while (resultSet.next())  
        {

            val timeString = resultSet.getString("time")
            val format = SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
            val time = java.sql.Timestamp(format.parse(timeString).time)

            println(resultSet.getString(1) + " " + resultSet.getString("column_name") + " " +
                     time.toString() + " " + resultSet.getDouble("value") )
        }
    }


    fun close()
    {
        conn.close()
    }
}