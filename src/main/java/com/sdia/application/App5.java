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

public class App5 {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession ss=SparkSession.builder().appName("Sockets").master("local[*]").getOrCreate();

        Dataset<Row> df =ss.readStream().format("socket").option("host","localhost")
                .option("port",8080)
                .load();

        Dataset<String> inputs=df.as(Encoders.STRING())
                .flatMap((FlatMapFunction<String,String>) line-> Arrays.asList(line.split(" ")).iterator(),Encoders.STRING());

        Dataset<Row> wordCount=inputs.groupBy("value").count();

        StreamingQuery query=wordCount.writeStream().outputMode("complete").format("console").start();
        query.awaitTermination();
    }
}
