package com.Spark.Demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ReadJson {
    @SuppressWarnings("resource")
    public static void main(String[] args){
        Logger.getLogger("org.apache").setLevel(Level.WARNING);
        SparkSession spark =SparkSession.builder().appName("testingSql").master("local[*}")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/").getOrCreate();
        Dataset<Row> empDF = spark.read().json("C:\\Users\\Administrator\\Desktop\\Data\\customerData.json");

        empDF.show();
        empDF.printSchema();
        System.out.println("SELECT Demo :");
        empDF.filter((empDF.col("salary"))).show();
    }
}
