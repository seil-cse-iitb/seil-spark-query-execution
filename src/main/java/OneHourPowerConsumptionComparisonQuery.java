import handler.ConfigHandler;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.util.Properties;

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

        Dataset<Row> sch_3 = getRowsByTableName(sparkSession, "sch_3");
        sch_3 = sch_3.where("TS>=1530037800 and TS<1530055800 and sensor_id='power_k_seil_p'");
        DatabaseStream ds = new DatabaseStream(sch_3, "1 hour", "1 minute", "TS", 1,
                functions.avg("W"), null, null, 1530041400, 75);


//		sch_3.select(functions.window(timestamp,"1 hour","1 minute")).printSchema();
//		sch_3.groupBy(functions.col("sensor_id"),functions.window(timestamp,"1 hour","1 minute")).avg("W").sort("window").show(1000,false);
    }
}
