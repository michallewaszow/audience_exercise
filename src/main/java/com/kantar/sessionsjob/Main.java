package com.kantar.sessionsjob;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.platform.commons.util.Preconditions;

import com.kantar.sessionsjob.config.SparkContextConfiguration;
import com.kantar.sessionsjob.io.PSVReader;
import com.kantar.sessionsjob.io.PSVWriter;

import java.io.File;

public class Main {

    public static void main(String[] args) {
        final String output = System.getProperty("output");
        final String input = System.getProperty("input");
        final String sparkAddress = System.getProperty("sparkAddress", "local[*]");

        Preconditions.notBlank(input, "-Dinput java argument cannot be empty");
        Preconditions.notBlank(output, "-Doutput java argument cannot be empty");

        final SQLContext sqlContext = SparkContextConfiguration.getSQLContext("sessionsJob", sparkAddress);
        final SessionsProcessor processor = new SessionsProcessor();

        final Dataset<Row> inputData = PSVReader.toDataset(sqlContext, new File(input));
        final Dataset<Row> outputData = processor.process(inputData);
        PSVWriter.fromDataset(outputData, output);
    }
}
