package com.Spark.Demo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Assignment4 {
    public static void main(String args[]) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> inputFile = sc.textFile("C:\\Users\\Administrator\\Desktop\\ebay.csv");
        JavaRDD<String> words = inputFile.flatMap(line -> Arrays.asList(line.split(";")).iterator());
       /* sc.parallelize(inputFile)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                .reduceByKey((value1, value2)-> value1 + value2)
                .foreach(tuple-> System.out.println(tuple._1 + " has " + tuple._2 +  " instances"));
*/
        JavaRDD<String> sell1;
        System.out.println("First element of rdd: " + words.collect().get(0));

        System.out.println("First 5 elements of rdd");
        for(int i=0; i<5; i++){
            System.out.println(words.collect().get(i));
        }
        JavaRDD<String> words1 = inputFile.flatMap(line -> Arrays.asList(line.split(",")[1]).iterator());
        Double bidVal = 0.0;
        for(int x = 0; x< 10654 ; x++){
           bidVal = bidVal + Double.valueOf(words1.collect().get(x));
        }
        System.out.println("Bid total is: " + bidVal);

        sc.close();
    }
}
