package com.sdia.incidents;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class IncidentsManagement {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession ss=SparkSession.builder().appName("Incidents Management").master("local[*]").getOrCreate();

        StructType schema=new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType,false, Metadata.empty()),
                new StructField("titre", DataTypes.StringType,false, Metadata.empty()),
                new StructField("description", DataTypes.StringType,false, Metadata.empty()),
                new StructField("service", DataTypes.StringType,false, Metadata.empty()),
                new StructField("date", DataTypes.StringType,false, Metadata.empty()),
        });

        Dataset<Row> df=ss.readStream().option("header",true).schema(schema).csv("inputs");
        df = df.withColumn("date", to_date(col("date"), "yyyy/MM/dd"));

        //Show incidents by service
        StreamingQuery incidentByService=df.groupBy("service")
                .agg(count("*").alias("incidentByService"))
                .writeStream().outputMode("complete").format("console").start();
        incidentByService.awaitTermination();


        //show 2 years that have the most incidents
        Dataset<Row> df1 = df.withColumn("year", year(col("date")));

        StreamingQuery incidentByYear = df1.groupBy("year")
                .agg(count("*").alias("incidentByYear"))
                .orderBy(desc("incidentByYear"))
                .limit(2)
                .writeStream().outputMode("complete").format("console").start();
        incidentByYear.awaitTermination();
    }
}
