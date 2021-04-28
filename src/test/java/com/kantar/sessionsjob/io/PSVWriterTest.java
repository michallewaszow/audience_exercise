package com.kantar.sessionsjob.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.kantar.sessionsjob.config.SparkContextConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class PSVWriterTest {

    @Test
    public void shouldWritePSVFile() throws IOException {
        // given
        final SQLContext sqlContext = SparkContextConfiguration.getSQLContext("test", "local[*]");
        final File given = new File("src/test/resources/input-statements.psv");
        final Dataset<Row> df = PSVReader.toDataset(sqlContext, given);
        final String path = "src/test/resources/testWrittenFile";
        // when
        final Path renamedFile = PSVWriter.fromDataset(df, path).toPath();
        // then
        Assertions.assertTrue(Files.exists(renamedFile));
        Files.delete(renamedFile);
    }

}
