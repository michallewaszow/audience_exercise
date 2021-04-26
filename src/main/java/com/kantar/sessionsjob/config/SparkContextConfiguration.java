package com.kantar.sessionsjob.config;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class SparkContextConfiguration {

    public static SQLContext getSQLContext(final String appName, final String clusterAddress){
        final SparkSession session = SparkSession.builder().appName(appName).master(clusterAddress).getOrCreate();
        return session.sqlContext();
    }

}
