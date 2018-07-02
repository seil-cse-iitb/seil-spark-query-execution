import handler.ConfigHandler;
import handler.LogHandler;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class OneHourPowerConsumptionComparisonQuery {

    public static void logsOff() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }

    public static Properties getProperties(String username, String password) {
        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        return properties;
    }

    public static Dataset<Row> getRowsByTableName(SparkSession sparkSession, String tableName) {
        Properties properties = getProperties(ConfigHandler.SOURCE_MYSQL_USERNAME, ConfigHandler.SOURCE_MYSQL_PASSWORD);
        Dataset<Row> rows = sparkSession.read().jdbc(ConfigHandler.SOURCE_MYSQL_URL, tableName, properties);
        return rows;
    }

    public static void main(String[] args) throws Exception {
        logsOff();
        SparkSession sparkSession = SparkSession.builder().appName("Java Spark App")
                .config("spark.sql.warehouse.dir", "~/spark-warehouse")
                .config("spark.executor.memory", "2g")
                .config("spark.driver.allowMultipleContexts", "true")
                .master("local[*]")
                .getOrCreate();

        ArrayList<Column> groupByColumns = new ArrayList<Column>();
        groupByColumns.add(col("sensor_id"));
        Column[] exprs = new Column[]{avg("V1")};
        Dataset<Row> sch_3 = getRowsByTableName(sparkSession, "sch_3");
        sch_3 = sch_3.where("TS>="+tsToSeconds("2018-06-21 00:00:00")+" and TS<"+tsToSeconds("2018-06-30 00:00:00")+" and sensor_id in ('power_k_seil_p','power_k_seil_a')");
        DatabaseStream ds = new DatabaseStream(sch_3, "1 hour", "1 minute", "TS", 10,
                avg("W"), exprs, groupByColumns, tsToSeconds("2018-06-21 00:00:00"), 75);

//        LogHandler.logInfo("" + (ds.getAggregatedRows(tsToSeconds("2018-06-21 13:00:00"), null)[0]));
        LogHandler.logInfo("" + (ds.getAggregatedRows(tsToSeconds("2018-06-21 6:01:00"), null)[0]));
        LogHandler.logInfo("" + (ds.getAggregatedRows(tsToSeconds("2018-06-21 7:02:00"), null)[0]));
        LogHandler.logInfo("" + (ds.getAggregatedRows(tsToSeconds("2018-06-21 13:10:00"), null)[0]));
        LogHandler.logInfo("" + (ds.getAggregatedRows(tsToSeconds("2018-06-21 13:50:00"), null)[0]));
        LogHandler.logInfo("" + (ds.getAggregatedRows(tsToSeconds("2018-06-21 18:00:00"), null)[0]));
        LogHandler.logInfo("" + (ds.getAggregatedRows(tsToSeconds("2018-06-24 22:51:00"), null)[0]));
        LogHandler.logInfo("" + (ds.getAggregatedRows(tsToSeconds("2018-06-24 03:20:00"), null)[0]));
        LogHandler.logInfo("" + (ds.getAggregatedRows(tsToSeconds("2018-06-24 14:30:00"), col("sensor_id").equalTo("power_k_seil_a"))[0]));


    }

    private static DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //can be static?

    public static String dateFormatter(double time) {
        return dateFormatter.format(new Date((long) (time * 1000)));
    }

    public static Integer tsToSeconds(String timestamp) {
        if (timestamp == null) return null;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date dt = sdf.parse(timestamp);
            long epoch = dt.getTime();
            return (int) (epoch / 1000);
        } catch (ParseException e) {
            return null;
        }
    }
}