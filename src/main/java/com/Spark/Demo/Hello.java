package com.Spark.Demo;
        import java.util.ArrayList;
        import java.util.Arrays;
        import java.util.List;

        import org.apache.log4j.Level;
        import org.apache.log4j.Logger;
        import org.apache.spark.SparkConf;
        import org.apache.spark.api.java.JavaRDD;
        import org.apache.spark.api.java.JavaSparkContext;
        import org.apache.spark.sql.sources.In;
        import scala.Tuple2;
        import scala.Tuple3;


public class Hello {
    public static int evenOrOdd (int value)
    {
        if(value%2 == 0){
            return value*value;
        }else{
            return value * value * value;
        }
    }
    public static void main(String args[]) throws InterruptedException {

List<Integer> input_data=new ArrayList<>();
		input_data.add(20);

		input_data.add(21);
		input_data.add(22);
		input_data.add(23);
		input_data.add(24);
		input_data.add(25);



        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> d = Arrays.asList(3,6,3,4,8);
        JavaRDD<Integer> collData = sc.parallelize(d);
        System.out.println("Data from RDD");
        collData.collect().forEach(System.out::println);
        System.out.println("Distinct Example: " + collData.distinct().collect());


        //////////////////////////////////////////////
        JavaRDD<String> inputFile = sc.textFile("C:\\Users\\Administrator\\Desktop\\abc.txt");
        JavaRDD<String> words = inputFile.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        System.out.println("Flat map: " + words.collect());
        ////////////////

        JavaRDD<Integer> myRdd = sc.parallelize(input_data);
        JavaRDD<Integer> sqRdd = myRdd.map(value -> evenOrOdd(value));

        JavaRDD<Tuple3<Integer, Integer,Double>> result = myRdd.map(value -> new Tuple3<>(value, value*value,Math.sqrt(value)));

        myRdd.collect().forEach(System.out::println);
        result.collect().forEach(System.out::println);
      //  result.saveAsTextFile("C:\\Users\\Administrator\\Desktop\\result.txt");
        JavaRDD<String> output = sc.textFile("C:\\Users\\Administrator\\Desktop\\result.txt");
        output.saveAsTextFile("C:\\Users\\Administrator\\Desktop\\result\\output.txt");

        String header = output.first();
        JavaRDD<String> autoData = output.filter(s-> !s.equals(header));
        JavaRDD<String> data = autoData.filter(s-> s.contains("toyota"));
        System.out.println(data.count());
        System.out.println(myRdd.count());
        sc.close();


    }

}


