import handler.ConfigHandler;
import handler.LogHandler;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.sql.Struct;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class DatabaseStream {
    //TODO test it on cluster

    private Dataset<Row> dataset;
    private Dataset<Row> slidingDataset;
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
     * i.e. (this.currentStreamEndTime - lastQueryTime - windowDurationInSeconds) <= ((100 - fetchNextWindowsAfterInPercent) / 100 * windowDurationInSeconds))
     * then fetch the next $noOfWindowToFetchAtOnce windows and union it with $dataset. Also update $currentStreamEndTime as described above.
     *
     * Delete a $windowDuration data when all the data of that window is consumed. i.e. delete $currentStartTime to $currentStartTime + $windowDuration data if $queryTime is > $currentStartTime + $windowDuration and shift $currentStartTime = $currentStartTime + $windowDuration
     *
     * Save sliding window data calculated from $dataset into $slidingDataset whenever new dataset is added to $dataset calculate sliding window and replace $slidingDataset (future work can be to add only new sliding window formed from new data).
     * also delete faulty sliding windows formed in $slidingDataset i.e. first $windowDuration sliding windows and last $windowDuration sliding windows.
     *
     */

    public DatabaseStream(Dataset<Row> dataset, String windowDuration, String slidingDuration, String timeField, int noOfWindowToFetchAtOnce, Column expr, Column[] exprs, List<Column> groupByColumns, double streamStartTime, double fetchNextWindowsAfterInPercent) throws Exception {
        this.dataset = dataset;
        this.windowDuration = windowDuration;
        this.slidingDuration = slidingDuration;
        this.timeField = timeField;
        this.noOfWindowToFetchAtOnce = noOfWindowToFetchAtOnce;
        this.expr = expr;
        this.exprs = exprs;
        this.streamStartTime = streamStartTime;
        this.fetchNextWindowsAfterInPercent = fetchNextWindowsAfterInPercent;
//some variable intialisation
        this.windowDurationInSeconds = durationInSeconds(windowDuration);
        this.slidingDurationInSeconds = durationInSeconds(slidingDuration);
        this.timestamp = functions.col(timeField).cast(DataTypes.LongType).cast(DataTypes.TimestampType).as("eventTime");
//some initial calculation
        if (groupByColumns == null) {
            groupByColumns = new ArrayList<Column>();
        }
        groupByColumns.add(functions.window(this.timestamp, this.windowDuration, this.slidingDuration));
        this.groupByColumns = new Column[groupByColumns.size()];
        groupByColumns.toArray(this.groupByColumns);

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

        Dataset<Row> dataset = this.dataset.where(timeField + ">=" + currentStreamStartTime + " and " + timeField + "<" + currentStreamEndTime);
        if (exprs == null)
            slidingDataset = dataset.groupBy(groupByColumns).agg(expr);
        else
            slidingDataset = dataset.groupBy(groupByColumns).agg(expr, exprs);
//        slidingDataset.unpersist(false); //TODO see memory usage because of not unpersisting.
        slidingDataset = slidingDataset.where(col("window.start").$greater$eq(currentStreamStartTimeStr)
                .and(col("window.end").$less(currentStreamEndTimeStr)));
        slidingDataset.persist(StorageLevel.MEMORY_ONLY());
        LogHandler.logInfo("[InitialSlidingDataset][currentStreamStartTime:" + currentStreamStartTimeStr + "][currentStreamEndTime:" + currentStreamEndTimeStr + "]");
        return slidingDataset;
    }

    public Row[] getAggregatedRows(long queryTime, Column where) {//queryTime in seconds
        //should be a atomic function (if different threads access the same object of DatabaseStream at same time than it might give faulty results)
        LogHandler.logInfo("[QueryTime]" + dateFormatter(queryTime));

        //TODO fetch Whatever Query is given if it is not currently calculated.
        fetchNextWindow(); //First fetch then query if more than $fetchNextWindowsAfterInPercent data from last window is used.

        //  TODO use structure for window column. This might make search faster
//        StructField[] structFields = {new StructField("start", DataTypes.TimestampType, false, null), new StructField("end", DataTypes.TimestampType, false, null)};
//        StructType s = new StructType(structFields);
        Row[] collect = null;
        if (where == null)
            collect = (Row[]) this.slidingDataset.where(col("window.start").equalTo(dateFormatter(queryTime))).collect();
        else
            collect = (Row[]) this.slidingDataset.where(col("window.start").equalTo(dateFormatter(queryTime)).and(where)).collect();
        this.lastQueryTime = queryTime;

        deletePreviousWindow();

        if (ConfigHandler.DEBUG == true && collect.length == 0) {
            LogHandler.logInfo("[LastQueryTime]" + dateFormatter(this.lastQueryTime));
            this.slidingDataset.show(200, false);
        }
        return collect;
    }

    private void deletePreviousWindow() {
        //TODO delete all previous windows. Currently deleting only 1 window if $queryTime is greater than ($currentStreamStartTime + $windowDurationInSeconds)
        //currently optimised for space. Can be optimised for time by not deleting a $windowDuration data instead delete
        // $noOfWindowToFetchAtOnce $windowDuration data at once
        if (lastQueryTime >= (currentStreamStartTime + windowDurationInSeconds)) {
            currentStreamStartTime += windowDurationInSeconds;
            currentStreamStartTimeStr = dateFormatter(currentStreamStartTime);

            dataset = dataset.where(col(timeField).$greater$eq(currentStreamStartTime));
//            slidingDataset.unpersist(false);
            slidingDataset = slidingDataset.where(col("window.start").$greater$eq(currentStreamStartTimeStr));
            slidingDataset.persist(StorageLevel.MEMORY_ONLY());

            LogHandler.logInfo("[DeletedPreviousWindow][currentStreamStartTime:" + currentStreamStartTimeStr + "]");
        } else {
//            LogHandler.logInfo("[!DeletedPreviousWindow][currentStreamStartTime:" + currentStreamStartTimeStr + "][currentStreamEndTime:" + currentStreamEndTimeStr + "]");
        }
    }

    private void fetchNextWindow() {

        if ((this.currentStreamEndTime - lastQueryTime - windowDurationInSeconds) <= ((100 - fetchNextWindowsAfterInPercent) / 100 * windowDurationInSeconds)) {
            double currentStreamStartTime = this.currentStreamEndTime - windowDurationInSeconds;
            double currentStreamEndTime = (currentStreamStartTime + (noOfWindowToFetchAtOnce + 1) * windowDurationInSeconds);

            Dataset<Row> dataset = this.dataset.where(timeField + ">=" + currentStreamStartTime + " and " + timeField + "<" + currentStreamEndTime);
            Dataset<Row> slidingDataset = null;
            if (exprs == null)
                slidingDataset = dataset.groupBy(groupByColumns).agg(expr);
            else
                slidingDataset = dataset.groupBy(groupByColumns).agg(expr, exprs);
            slidingDataset = slidingDataset.where(col("window.start").$greater$eq(dateFormatter(currentStreamStartTime))
                    .and(col("window.end").$less(dateFormatter(currentStreamEndTime))));
//            this.slidingDataset.unpersist(false);
            this.slidingDataset = this.slidingDataset.union(slidingDataset);
            this.slidingDataset.persist(StorageLevel.MEMORY_ONLY());

            this.currentStreamEndTime = currentStreamEndTime;
            this.currentStreamEndTimeStr = dateFormatter(this.currentStreamEndTime);

            LogHandler.logInfo("[FetchedNextWindow][currentStreamEndTime:" + currentStreamEndTimeStr + "]");
        } else {
//            LogHandler.logInfo("[!FetchedNextWindow][currentStreamStartTime:" + currentStreamStartTimeStr + "][currentStreamEndTime:" + currentStreamEndTimeStr + "]");
        }
    }

    private String dateFormatter(double time) {
        return dateFormatter.format(new Date((long) (time * 1000)));
    }

    private double durationInSeconds(String duration) throws Exception {
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
