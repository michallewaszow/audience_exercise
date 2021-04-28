package com.kantar.sessionsjob.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.File;

public class PSVReader {

    public static Dataset<Row> toDataset(final SQLContext sqlContext, final File file){
        return sqlContext.read()
                         .option("sep", "|")
                         .option("header", "true")
                         .csv("src/test/resources/input-statements.psv");
    }

}
