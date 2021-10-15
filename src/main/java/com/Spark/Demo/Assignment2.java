package com.Spark.Demo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Assignment2 {
    public static void main(String args[]) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> output = sc.textFile("C:\\Users\\Administrator\\Desktop\\auto-Data.csv");
        output.saveAsTextFile("C:\\Users\\Administrator\\Desktop\\result\\auto-data.csv.txt");

        String header = output.first();
        JavaRDD<String> autoData = output.filter(s-> !s.equals(header));
        JavaRDD<String> data = autoData.filter(s-> s.contains("toyota"));

        System.out.println(data.count());
        //System.out.println(myRdd.count());
        sc.close();

    }

}
