import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.util.Arrays;
import java.util.List;

public class DatabaseStream {
	private Column timestamp;
	private String windowDuration;
	private String slidingDuration;
	private String timeField;
	private double startTime;
	private Dataset dataset;
	private Dataset slidingDataset;

	public DatabaseStream(){}

	private Dataset startStream(Column expr, Column[] exprs, Column... groupByColumns) throws Exception {
		//This will fetch 2 windowDuration of data from TS>=startTime
		//convert that 2 windowDuration of data into grouped by
		//Columns... passed as parameters and (window with windowDuration and slidingDuration)
		dataset = dataset.where(timeField + ">=" + startTime+" and "+timeField+"<"+(startTime+2*toSeconds(windowDuration)));
		List<Column> columns = Arrays.asList(groupByColumns);
		columns.add(functions.window(this.timestamp,windowDuration,slidingDuration));
		slidingDataset = dataset.groupBy((Column[]) columns.toArray()).agg(expr,exprs);
		return slidingDataset;
	}
//should there be only next function??
	public Dataset getAggDataset(Column window){
		//TODO: check if window is of data type Window which is a structure of two fields startTime and endTime
		return slidingDataset.where(window);
	}


	private double toSeconds(String duration) throws Exception {
		String[] split = duration.split(" ");
		int value = Integer.parseInt(split[0]);
		if(split[1].equalsIgnoreCase("day")){
			value*=3600*24;
		}else if(split[1].equalsIgnoreCase("hour")){
			value*=3600;
		}else if (split[1].equalsIgnoreCase("minute")){
			value*=60;
		}else if(split[1].equalsIgnoreCase("second")){
			value*=1;
		}else {
			throw new Exception("[MyCode][DatabaseStream::toSecond()] Invalid duration type. Currently support (day,hour,minute,second)");
		}
		return value;
	}

}
