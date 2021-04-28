package com.kantar.sessionsjob.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.kantar.sessionsjob.config.SparkContextConfiguration;

import java.io.File;

public class PSVReaderTest {

    @Test
    public void shouldReadPSVFile() {
        // given
        final SQLContext sqlContext = SparkContextConfiguration.getSQLContext("test", "local[*]");
        final Dataset<Row> df = PSVReader.toDataset(sqlContext, new File("src/test/resources/input-statements.psv"));
        final String[] expected = { "HomeNo", "Channel", "Starttime", "Activity" };
        // when
        final String[] actual = df.columns();
        // then
        Assertions.assertArrayEquals(expected, actual);
    }

}
