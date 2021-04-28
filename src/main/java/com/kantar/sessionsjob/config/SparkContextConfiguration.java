package com.kantar.sessionsjob.config;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class SparkContextConfiguration {

    private static SQLContext sqlContext;

    private SparkContextConfiguration() {
    }

    public static SQLContext getSQLContext(final String appName, final String clusterAddress) {
        if (sqlContext == null) {
            final SparkSession session = SparkSession.builder()
                                                     .appName(appName)
                                                     .master(clusterAddress)
                                                     .getOrCreate();
            sqlContext = session.sqlContext();
            log.info("SQLContext created.");
        } else {
            log.info("SQLContext already exists, passed parameters won't take effect.");
        }
        return sqlContext;
    }
}
