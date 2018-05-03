package handler;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class ConfigHandler {
	public final static String REPORT_RECEIVER_EMAIL = "sapantanted99@gmail.com";
	public final static String SCRIPT_IDENTITY_TEXT = "Spark script for aggregation";
	public final static String LOG_FILE_PATH = "./log";
	public final static String MYSQL_HOST = "10.129.149.11";
	//    public final  static String MYSQL_HOST = "mysql.seil.cse.iitb.ac.in";
	public final static String MYSQL_USERNAME = "root";
	public final static String MYSQL_PASSWORD = "MySQL@seil";
	public final static String MYSQL_DATABASE_NAME = "seil_sensor_data";
	public final static String MYSQL_URL = "jdbc:mysql://" + MYSQL_HOST + ":3306/" + MYSQL_DATABASE_NAME + "?useSSL=false";
	public final static long GRANULARITY_IN_SECONDS = 60;
	public final static long DATA_ROWS_PER_SECOND = 1;
	public final static String[] SQL_AGGREGATION_FORMULA_SCH_3 = {"first(sensor_id) as sensor_id", "first(srl) as srl", "avg(F) as F", "avg(W) as W", "avg(W1) as W1", "avg(W2) as W2", "avg(W3) as W3", "avg(V1) as V1", "avg(V2) as V2", "avg(V3) as V3", "avg(A) as A", "avg(A1) as A1", "avg(A2) as A2", "avg(A3) as A3", "avg(VAR) as VAR", "avg(VAR1) as VAR1", "avg(VAR2) as VAR2", "avg(VAR3) as VAR3", "avg(VA) as VA", "avg(VA1) as VA1", "avg(VA2) as VA2", "avg(VA3) as VA3", "(avg(W)/avg(VA)) as PF", "(avg(W1)/avg(VA1)) as PF1", "(avg(W2)/avg(VA2)) as PF2", "(avg(W3)/avg(VA3)) as PF3", "last(TS_RECV) as TS_RECV", "last(FwdWh) as FwdVAh", "last(FwdWh) as FwdVARhC", "last(FwdWh) as FwdVARhR", "last(FwdWh) as FwdWh", "(last(FwdWh)-first(FwdWh)) as delta_FwdWh", "(count(*)/" + (ConfigHandler.GRANULARITY_IN_SECONDS / ConfigHandler.DATA_ROWS_PER_SECOND) + "*100) as data_percent",
	};
	public final static StructType SCH_3_SCHEMA = new StructType()
			.add(DataTypes.createStructField("sensor_id", StringType, false))
			.add(DataTypes.createStructField("TS_RECV", DoubleType, false))
			.add(DataTypes.createStructField("srl", DoubleType, true))
			.add(DataTypes.createStructField("TS", DoubleType, true))
			.add(DataTypes.createStructField("VA", DoubleType, true))
			.add(DataTypes.createStructField("W", DoubleType, true))
			.add(DataTypes.createStructField("VAR", DoubleType, true))
			.add(DataTypes.createStructField("PF", DoubleType, true))
			.add(DataTypes.createStructField("VLL", DoubleType, true))
			.add(DataTypes.createStructField("VLN", DoubleType, true))
			.add(DataTypes.createStructField("A", DoubleType, true))
			.add(DataTypes.createStructField("F", DoubleType, true))
			.add(DataTypes.createStructField("VA1", DoubleType, true))
			.add(DataTypes.createStructField("W1", DoubleType, true))
			.add(DataTypes.createStructField("VAR1", DoubleType, true))
			.add(DataTypes.createStructField("PF1", DoubleType, true))
			.add(DataTypes.createStructField("V12", DoubleType, true))
			.add(DataTypes.createStructField("V1", DoubleType, true))
			.add(DataTypes.createStructField("A1", DoubleType, true))
			.add(DataTypes.createStructField("VA2", DoubleType, true))
			.add(DataTypes.createStructField("W2", DoubleType, true))
			.add(DataTypes.createStructField("VAR2", DoubleType, true))
			.add(DataTypes.createStructField("PF2", DoubleType, true))
			.add(DataTypes.createStructField("V23", DoubleType, true))
			.add(DataTypes.createStructField("V2", DoubleType, true))
			.add(DataTypes.createStructField("A2", DoubleType, true))
			.add(DataTypes.createStructField("VA3", DoubleType, true))
			.add(DataTypes.createStructField("W3", DoubleType, true))
			.add(DataTypes.createStructField("VAR3", DoubleType, true))
			.add(DataTypes.createStructField("PF3", DoubleType, true))
			.add(DataTypes.createStructField("V31", DoubleType, true))
			.add(DataTypes.createStructField("V3", DoubleType, true))
			.add(DataTypes.createStructField("A3", DoubleType, true))
			.add(DataTypes.createStructField("FwdVAh", DoubleType, true))
			.add(DataTypes.createStructField("FwdWh", DoubleType, true))
			.add(DataTypes.createStructField("FwdVARhR", DoubleType, true))
			.add(DataTypes.createStructField("FwdVARhC", DoubleType, true));
	//TODO define aggregation formula for all the database tables

	//Live Aggregation Variables;
//    public final  static String MQTT_HOST = "10.129.149.9";
	public final static String MQTT_HOST = "mqtt.seil.cse.iitb.ac.in";
	public final static String MQTT_URL = "tcp://" + MQTT_HOST + ":1883";
	public final static long LIVE_AGGREGATION_INTERVAL_IN_SECONDS = 10;//TODO test when LIVE_AGGREGATION_INTERVAL_IN_SECONDS < LIVE_GRANULARITY_IN_SECONDS
	public final static long LIVE_GRANULARITY_IN_SECONDS = 10;
	//TODO define schema for all the database tables


	//Common Variables;
	public final static boolean REPORT_ERROR = false;
}
