package com.sdia.application;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class App1 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf=new SparkConf().setAppName("NetCat").setMaster("localhost(*)");
        JavaStreamingContext streamingContext=new JavaStreamingContext(sparkConf, Durations.seconds(8));
        JavaReceiverInputDStream<String> inputDStream=streamingContext.socketTextStream("localhost",8080);
        inputDStream.print();
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
