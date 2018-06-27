import handler.LogHandler;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import sun.rmi.runtime.Log;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class DatabaseStream {


    private Dataset dataset;
    private Dataset slidingDataset;
    private String windowDuration;
    private String slidingDuration;
    private String timeField;
    private int noOfWindowToFetchAtOnce = 1;
    private Column expr;
    private Column[] exprs;
    private Column[] groupByColumns;
    private double streamStartTime = -1; //(in seconds)
    private double lastQueryTime = -1;
    private double currentStreamStartTime = -1;
    private double currentStreamEndTime = -1;
    private double fetchNextWindowsAfterInPercent = 75; // 75% of last windows is consumed then fetch new data from db
    private boolean isStreamStarted = false;
    private double windowDurationInSeconds;
    private double slidingDurationInSeconds;
    private Column timestamp;

    //Meta
    private DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //can be static?
    private String currentStreamStartTimeStr;//always update with currentStreamStartTime
    private String currentStreamEndTimeStr; //always update with currentStreamEndTime

    /*
     * Requirement: Give me a database stream of window $windowDuration sliding with $slidingDuration
     * on dataset $dataset, stream starting from time $streamStartTime where time column is $timeField.
     * Since group by clause is needed to perform windowing function, aggregation expressions $expr(not nullable), $exprs[](nullable) are also required.
     *
     * I will give you a $queryTime: you send me aggregated data of window from stream where $queryTime = start time of window.
     * $queryTime will always be increasing, it will not go back so data before $queryTime can be deleted.
     *
     * $currentStreamStartTime is start time of the current($noOfWindowToFetchAtOnce * $windowDuration) stream remaining in Spark.
     * $currentStreamEndTime is end time of the current($noOfWindowToFetchAtOnce * $windowDuration) stream remaining in Spark. (calculated using $currentStreamEndTime + $noOfWindowToFetchAtOnce * $windowDuration)
     * till $currentStreamEndTime proper window are available i.e. removing last 1 $windowDuration sliding windows. Update $currentStreamEndTime when you union new dataset
     *
     * At a specific time, current stream can have max ($noOfWindowToFetchAtOnce + 1) * $windowDuration of data.
     *
     * When I give you $queryTime which is $fetchNextWindowsAfterInPercent% percent of the last window available in stream
     * i.e. ($currentStreamEndTime - $queryTime) >= ($fetchNextWindowsAfterInPercent / 100 * $windowDuration)
     * then fetch the next $noOfWindowToFetchAtOnce windows and union it with $dataset. Also update $currentStreamEndTime as described above.
     *
     * Delete a $windowDuration data when all the data of that window is consumed. i.e. delete $currentStartTime to $currentStartTime + $windowDuration data if $queryTime is > $currentStartTime + $windowDuration and shift $currentStartTime = $currentStartTime + $windowDuration
     *
     * Save sliding window data calculated from $dataset into $slidingDataset whenever new dataset is added to $dataset calculate sliding window and replace $slidingDataset (future work can be to add only new sliding window formed from new data).
     * also delete faulty sliding windows formed in $slidingDataset i.e. first $windowDuration sliding windows and last $windowDuration sliding windows.
     *
     */

    public DatabaseStream(Dataset dataset, String windowDuration, String slidingDuration, String timeField, int noOfWindowToFetchAtOnce, Column expr, Column[] exprs, Column[] groupByColumns, double streamStartTime, double fetchNextWindowsAfterInPercent) throws Exception {
        this.dataset = dataset;
        this.windowDuration = windowDuration;
        this.slidingDuration = slidingDuration;
        this.timeField = timeField;
        this.noOfWindowToFetchAtOnce = noOfWindowToFetchAtOnce;
        this.expr = expr;
        this.exprs = exprs;
        this.groupByColumns = groupByColumns;
        this.streamStartTime = streamStartTime;
        this.fetchNextWindowsAfterInPercent = fetchNextWindowsAfterInPercent;
//some variable intialisation
        this.windowDurationInSeconds = toSeconds(windowDuration);
        this.slidingDurationInSeconds = toSeconds(slidingDuration);
        this.timestamp = functions.col(timeField).cast(DataTypes.LongType).cast(DataTypes.TimestampType).as("eventTime");
//some initial calculation
        ArrayList<Column> columns;
        if (this.groupByColumns != null) {
            columns = (ArrayList<Column>) Arrays.asList(this.groupByColumns);
        } else {
            this.groupByColumns = new Column[1];
            columns = new ArrayList<Column>();
        }

        columns.add(functions.window(this.timestamp, this.windowDuration, this.slidingDuration));
        columns.toArray(this.groupByColumns);

        if (this.expr == null) {
            throw new Exception("[MyException][expr can't be null]");
        }

        //Start Streaming
        intialiseStream();
    }

    private Dataset intialiseStream() {
        /*
         * This will fetch $noOfWindowToFetchAtOnce * $windowDuration of data from $dataset where $timeField >= $streamStartTime.
         * Convert that $noOfWindowToFetchAtOnce * $windowDuration of data into grouped by $groupByColumns passed as parameters
         * and (window with $windowDuration and $slidingDuration)
         */
        currentStreamStartTime = streamStartTime;
        currentStreamEndTime = currentStreamStartTime + (noOfWindowToFetchAtOnce + 1) * windowDurationInSeconds;
        currentStreamStartTimeStr = dateFormatter(currentStreamStartTime);
        currentStreamEndTimeStr = dateFormatter(currentStreamEndTime);

        dataset = dataset.where(timeField + ">=" + currentStreamStartTime + " and " + timeField + "<" + currentStreamEndTime);
        if (exprs == null)
            slidingDataset = dataset.groupBy(groupByColumns).agg(expr);
        else
            slidingDataset = dataset.groupBy(groupByColumns).agg(expr, exprs);
        slidingDataset = slidingDataset.where(col("window.start").$greater$eq(currentStreamStartTimeStr).
                and(col("window.end").$less(currentStreamEndTimeStr)));

//        slidingDataset.show(200, false);
        LogHandler.logInfo("Initial Sliding Dataset Count:" + slidingDataset.count());
        return slidingDataset;
    }

    private Dataset startStream(Column expr, Column[] exprs, Column[] groupByColumns) throws Exception {
        /*
         * This will fetch 2 windowDuration of data from TS >= streamStartTime.
         * Convert that 2 windowDuration of data into grouped by Columns... passed as parameters
         * and (window with windowDuration and slidingDuration)
         */
        dataset = dataset.where(timeField + ">=" + streamStartTime + " and " + timeField + "<" + (streamStartTime + (noOfWindowToFetchAtOnce + 1) * toSeconds(windowDuration)));
        List<Column> columns = Arrays.asList(groupByColumns);
        columns.add(functions.window(this.timestamp, windowDuration, slidingDuration));
        slidingDataset = dataset.groupBy((Column[]) columns.toArray()).agg(expr, exprs);

        slidingDataset.show();
        return slidingDataset;
    }


    //should there be only next function??
    public Dataset getAggDataset(Column window) {
        //TODO: check if window is of data type Window which is a structure of two fields streamStartTime and endTime
        return slidingDataset.where(window);
    }

    private String dateFormatter(double time) {
        return dateFormatter.format(new Date((long) (time * 1000)));
    }

    private double toSeconds(String duration) throws Exception {
        String[] split = duration.split(" ");
        int value = Integer.parseInt(split[0]);
        if (split[1].equalsIgnoreCase("day")) {
            value *= 3600 * 24;
        } else if (split[1].equalsIgnoreCase("hour")) {
            value *= 3600;
        } else if (split[1].equalsIgnoreCase("minute")) {
            value *= 60;
        } else if (split[1].equalsIgnoreCase("second")) {
            value *= 1;
        } else {
            throw new Exception("[MyException][DatabaseStream::toSecond()] Invalid duration type. Currently supported types are: (day,hour,minute,second)");
        }
        return value;
    }

}
//    Date date = new Date((long) (this.streamStartTime * 1000));
