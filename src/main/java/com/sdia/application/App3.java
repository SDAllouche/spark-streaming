package com.sdia.application;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class App3 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf=new SparkConf().setAppName("WordCount HDFS").setMaster("local[*]");
        JavaStreamingContext streamingContext=new JavaStreamingContext(sparkConf, Durations.seconds(8));
        JavaDStream<String> inputDStream=streamingContext.textFileStream("hdfs://localhost:9000/repo");
        JavaDStream<String> dStream=inputDStream.flatMap(line-> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String,Integer> pairDStream1=dStream.mapToPair(m->new Tuple2<>(m,1));
        JavaPairDStream pairDStream2=pairDStream1.reduceByKey((a,b)->a+b);
        pairDStream2.print();
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
