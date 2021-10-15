package com.Spark.Demo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json4s.RootStreamingJsonWriter;
import scala.Tuple2;

import java.util.Arrays;

public class Assignment1 {

    public static void main(String args[]) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> inputFile = sc.textFile("C:\\Users\\Administrator\\Desktop\\Assignment1.txt");

       sc.parallelize(inputFile.collect())
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                .reduceByKey((value1, value2)-> value1 + value2)
                .foreach(tuple-> System.out.println(tuple._1 + " has " + tuple._2 +  " instances"));
        JavaRDD<String> step2 = sc.textFile("C:\\Users\\Administrator\\Desktop\\abc1.txt");
        JavaRDD<String> words = step2.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new scala.Tuple2<String, Integer>(word, 1));
        pairs.cache();
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a,b)-> a+b);
        counts.foreach(elements -> System.out.println(elements));
       /* while(true){
            try{
                Thread.sleep(10000);
            }catch(InterruptedException e){
                e.printStackTrace();
            }
        }*/
        SparkSession spark =SparkSession.builder().appName("testingSql").master("local[*}")
                        .config("spark.sql.warehouse.dir","file:///c:/tmp").getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("C:\\Users\\Administrator\\Desktop\\Data\\students.csv");
        dataset.show();
        long num_of_records = dataset.count();
        System.out.println("There are " + num_of_records + "rows");

        Dataset<Row> filtered = dataset.filter("subject='Modern Art' AND year>=7");
        filtered.show();

        dataset.createOrReplaceGlobalTempView("my_students_table");
        Dataset<Row> results = spark.sql("select * from my_students_table where subject = 'Modern Art' AND year >= 7");
        results.show();
        Dataset<Row> results1 = spark.sql("select * from my_students_table where subject = 'Modern Art' AND year >= 7 order by year descending");
        results1.show();
        spark.close();
    }

}
