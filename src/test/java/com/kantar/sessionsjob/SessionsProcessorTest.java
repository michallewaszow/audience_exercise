package com.kantar.sessionsjob;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.kantar.sessionsjob.config.SparkContextConfiguration;
import com.kantar.sessionsjob.io.PSVReader;
import com.kantar.sessionsjob.io.PSVWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class SessionsProcessorTest {

    private static SQLContext SQL_CONTEXT;

    @BeforeAll
    static void setup() {
        SQL_CONTEXT = SparkContextConfiguration.getSQLContext("test", "local[*]");
    }

    @Test
    public void shouldContainOriginalAndAppendedColumnsAfterProcessing() throws IOException {
        // given
        final SessionsProcessor processor = new SessionsProcessor();
        final File given = new File("src/test/resources/input-statements.psv");
        final Dataset<Row> df = PSVReader.toDataset(SQL_CONTEXT, given);
        final String[] expectedColumns = new String[] { "HomeNo",
                                                        "Channel",
                                                        "Starttime",
                                                        "Activity",
                                                        "EndTime",
                                                        "Duration" };
        // when
        final Dataset<Row> result = processor.process(df);
        // then
        Assertions.assertArrayEquals(result.columns(), expectedColumns, "Column should be equal");
    }

    @Test
    public void processedFileShouldHaveExpectedSize() throws IOException {
        // given
        final SessionsProcessor processor = new SessionsProcessor();
        final File given = new File("src/test/resources/input-statements.psv");
        final File expected = new File("src/test/resources/expected-sessions.psv");
        final Dataset<Row> df = PSVReader.toDataset(SQL_CONTEXT, given);

        // when
        final Dataset<Row> result = processor.process(df);
        final File actual = PSVWriter.fromDataset(result, "result");
        // then
        Assertions.assertEquals(expected.length(), actual.length());
        Files.delete(actual.toPath());
    }

}
