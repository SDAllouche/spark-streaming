package com.sdia.incidents;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.*;

public class IncidentsManagement {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().appName("Incidents Management").master("local[*]").getOrCreate();

        StructType schema=new StructType(new StructField[]{
                new StructField("id", DataTypes.LongType,false, Metadata.empty()),
                new StructField("titre", DataTypes.StringType,false, Metadata.empty()),
                new StructField("description", DataTypes.StringType,false, Metadata.empty()),
                new StructField("service", DataTypes.StringType,false, Metadata.empty()),
                new StructField("date", DataTypes.DateType,false, Metadata.empty()),
        });

        Dataset<Row> df=ss.read().option("header",true).schema(schema).csv("inputs");
        df = df.withColumn("date", to_date(col("date"), "yyyy/MM/dd"));
        df.show();

        //Show incidents by service
        Dataset<Row> incidentByService = df.groupBy("service").agg(count("*").alias("incidentByService"));
        incidentByService.show();

        //show 2 years that have the most incidents
        Dataset<Row> df1 = df.withColumn("year", year(col("date")));
        df1.show();

        Dataset<Row> incidentByYear = df1.groupBy("year")
                .agg(count("*").alias("incidentByYear"))
                .orderBy(desc("incidentByYear"))
                .limit(2);

        incidentByYear.show();
    }
}
