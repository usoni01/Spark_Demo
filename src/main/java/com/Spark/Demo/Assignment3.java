package com.Spark.Demo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;


public class Assignment3 {
    public static void main(String args[]) throws InterruptedException {

        List<String> input_data=new ArrayList<>();
        input_data.add("WARN: client stopped connection");
        input_data.add("FATAL: GET request failed");
        input_data.add("WARN: client stopped connection");
        input_data.add("ERROR: Incorrect URL");
        input_data.add("ERROR: POST request failed");
        input_data.add("FATAL: File does not exist");
        input_data.add("ERROR: File does not exist");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.parallelize(input_data)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                .reduceByKey((value1, value2)-> value1 + value2)
                .foreach(tuple-> System.out.println(tuple._1 + " has " + tuple._2 +  " instances"));

        JavaRDD<String> count = sc.parallelize(input_data);
        JavaRDD<String> data = count.filter(s-> s.contains ("WARN"));
        data.collect().forEach(System.out::println);
        JavaRDD<String> length = count.filter(s-> s.length() > 5);
        System.out.println("Greater than 5: ");  length.collect().forEach(System.out::println);
        sc.close();
}
}
