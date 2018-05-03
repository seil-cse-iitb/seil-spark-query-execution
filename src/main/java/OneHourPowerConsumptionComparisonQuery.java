import handler.ConfigHandler;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class OneHourPowerConsumptionComparisonQuery {

	public static void logsOff() {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
	}

	public static Properties getProperties() {
		Properties properties = new Properties();
		properties.setProperty("user", ConfigHandler.MYSQL_USERNAME);
		properties.setProperty("password", ConfigHandler.MYSQL_PASSWORD);
		return properties;
	}

	public static Dataset<Row> getRowsByTableName(SparkSession sparkSession,String tableName) {
		Properties properties = getProperties();
		Dataset<Row> rows = sparkSession.read().jdbc(ConfigHandler.MYSQL_URL, tableName, properties);
		return rows;
	}
	public static void main(String[] args) {
		logsOff();
		SparkSession sparkSession = SparkSession.builder().appName("Java Spark Demo")
				.config("spark.sql.warehouse.dir", "~/spark-warehouse")
				.config("spark.executor.memory", "2g")
				.config("spark.driver.allowMultipleContexts", "true")
				.master("local[4]")
				.getOrCreate();



	}
}
