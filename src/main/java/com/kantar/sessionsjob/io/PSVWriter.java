package com.kantar.sessionsjob.io;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

@Slf4j
public class PSVWriter {

    public static File fromDataset(final Dataset<Row> df, final String fileDirectory) {
        df.repartition(1).write().option("sep", "|").option("header", "true").csv(fileDirectory);
        final File directory = new File(fileDirectory);
        final File[] matchedFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));
        final File csvFile = Objects.requireNonNull(matchedFiles)[0];
        final File renamedFile = new File(fileDirectory + ".psv");
        final boolean renamed = csvFile.renameTo(renamedFile);
        log.info("File " + (renamed ? "renamed properly" : "wasn't renamed properly"));
        try {
            FileUtils.deleteDirectory(directory);
        } catch (IOException e) {
            log.error("Error during directory deletion. Error message: {}", e.getMessage());
        }
        return renamedFile;
    }

}
