package com.Spark.Demo;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.awt.*;
import java.util.ArrayList;
import java.util.List;

import static java.awt.SystemColor.desktop;

public class StringData {
    public static void main(String args[]) throws InterruptedException {

        List<String> input_data=new ArrayList<>();
        input_data.add("Error: one");

        input_data.add("Warning: two");

        input_data.add("Fatal: three");
        input_data.add("Fatal: three");
        input_data.add("Warning: three");



        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);


      /*  JavaRDD<String> readFile= sc.parallelize(input_data);
        JavaPairRDD<String, Long> rdd2 = readFile.mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L));
        rdd2.collect().forEach(value -> System.out.print(value));

        rdd2.groupByKey().collect().forEach(value-> System.out.println(value));

        JavaPairRDD<String, Iterable<Long>> rdd3 = rdd2.groupByKey();
        rdd3 = rdd3.sortByKey();

        rdd3.collect().forEach(aa -> System.out.println(aa._1 +" has "  + Iterables.size(aa._2) + " instansces"));
        //readFile.collect().forEach(System.out::println);*/

        sc.parallelize(input_data)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                        .reduceByKey((value1, value2)-> value1 + value2)
                                .foreach(tuple-> System.out.println(tuple._1 + " has " + tuple._2 +  " instances"));


     //   System.out.println(myRdd.count());
        sc.close();


    }

}
