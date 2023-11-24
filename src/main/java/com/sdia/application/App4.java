package com.sdia.application;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class App4 {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession ss=SparkSession.builder().appName("Sockets").master("local[*]").getOrCreate();

        Dataset<Row> df =ss.readStream().format("socket").option("host","localhost")
                .option("port",8080)
                .load();

        StreamingQuery query=df.writeStream().outputMode("append").format("console").start();
        query.awaitTermination();

    }
}
