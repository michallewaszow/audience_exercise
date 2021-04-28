package com.kantar.sessionsjob;

import static org.apache.spark.sql.functions.asc;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.from_unixtime;
import static org.apache.spark.sql.functions.lead;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.when;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public class SessionsProcessor {

    public static final String DATE_FORMAT = "yyyyMMddHHmmss";
    private static final String STARTTIME_COLNAME = "Starttime";
    public static final String ENDTIME_COLNAME = "EndTime";
    public static final String HOMENO_COLNAME = "HomeNo";
    public static final String DURATION_COLNAME = "Duration";

    public Dataset<Row> process(Dataset<Row> df) {
        df = withStarttimeToTimestamp(df);
        df = calculateEndtime(df);
        df = calculateDuration(df);
        return reformatData(df);
    }

    private Dataset<Row> withStarttimeToTimestamp(final Dataset<Row> df) {
        return df.withColumn(STARTTIME_COLNAME, to_timestamp(col(STARTTIME_COLNAME), DATE_FORMAT));
    }

    private Dataset<Row> calculateEndtime(final Dataset<Row> df) {
        final WindowSpec window = Window.partitionBy(HOMENO_COLNAME)
                                        .orderBy(asc(STARTTIME_COLNAME));
        final Column nextRowIsNull = lead(col(STARTTIME_COLNAME), 1).over(window)
                                                                    .isNull();
        final Column lastSecondOfDayInUnixtime = to_timestamp(date_add(col(STARTTIME_COLNAME), 1)).cast("long")
                                                                                                  .minus(1);
        final Column lastSecondOfDay = to_timestamp(from_unixtime(lastSecondOfDayInUnixtime));
        final Column secondBeforeNextStarttimeInUnixtime = lead(col(STARTTIME_COLNAME), 1).over(window)
                                                                                          .cast("long")
                                                                                          .minus(1);
        final Column endtime = to_timestamp(from_unixtime(secondBeforeNextStarttimeInUnixtime));
        return df.withColumn(ENDTIME_COLNAME, when(nextRowIsNull, lastSecondOfDay).otherwise(endtime));
    }

    private Dataset<Row> calculateDuration(final Dataset<Row> df) {
        Column endtimeStarttimeDiffInSeconds = col(ENDTIME_COLNAME).cast("long")
                                                                   .minus(col(STARTTIME_COLNAME).cast("long"));

        return df.withColumn(DURATION_COLNAME, endtimeStarttimeDiffInSeconds);
    }

    private Dataset<Row> reformatData(final Dataset<Row> df) {
        return df.withColumn("Starttime", date_format(col("Starttime"), DATE_FORMAT))
                 .withColumn("EndTime", date_format(col("EndTime"), DATE_FORMAT));
    }
}
