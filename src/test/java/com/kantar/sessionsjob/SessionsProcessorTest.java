package com.kantar.sessionsjob;

import com.kantar.sessionsjob.config.SparkContextConfiguration;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.LongType$;
import org.junit.jupiter.api.Test;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.date_add;

public class SessionsProcessorTest {


    @Test
    public void shouldGroupSessionsByDeviceId() {
        //given
        WindowSpec window = Window.partitionBy("HomeNo").orderBy(asc("Starttime"));

        Column nextRowIsNull = lead(col("Starttime"), 1).over(window).isNull();
        Column lastSecondOfDay = to_timestamp(from_unixtime(lit(to_timestamp(date_add(col("Starttime"), 1)).cast("long").minus(1))));
        Column endtime = to_timestamp(from_unixtime(lead(col("Starttime"), 1).over(window).cast("long").minus(1)));
        Column endtimeStarttimeDiffInSeconds = col("Endtime").cast("long").minus(col("Starttime").cast("long"));

        final SQLContext sqlContext = SparkContextConfiguration.getSQLContext("SessionsProcessor", "local[2]");
        Dataset<Row> df = sqlContext.read().option("sep", "|").option("header", "true").csv("src/test/resources/input-statements.psv");
        Dataset<Row> newDF = df.withColumn("Starttime", to_timestamp(col("Starttime"), "yyyyMMddHHmmss"));
        Dataset<Row> endtimes = newDF.withColumn("Endtime", when(nextRowIsNull, lastSecondOfDay).otherwise(endtime))
                                    .withColumn("Duration", endtimeStarttimeDiffInSeconds);
        endtimes.printSchema();
        endtimes.show();
    }


}
