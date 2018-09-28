package com.myspark.streaming;

import java.util.Arrays;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

import scala.Tuple2;

/**
 * 1. Try a Spark streaming example
 * ref: https://spark.apache.org/docs/latest/streaming-programming-guide.html
 * @author shijie
 *
 */
public class SparkStreamingExample {
    
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\\\temp\\winutil\\");
        
        // Create a local StreamingContext with two working thread and batch
        // interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000)); //Durations.seconds(10)

        // Create a DStream that will connect to hostname:port, like
        // localhost:9999
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
         
        // QUESTION: 除了socketTextStream还有几种输入stream？ actorStream, fileStream, queueStream, textFileStream, rawSocketStream
        // 官方直接支持的输出端有哪些 JDBC， HBase ？
        
        // Split each line into words
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        
        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words
                .mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                });
        JavaPairDStream<String, Integer> wordCounts = pairs
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) throws Exception {
                        return i1 + i2;
                    }
                });

        // Print the first ten elements of each RDD generated in this DStream to
        // the console
        wordCounts.print();

        jssc.start(); // Start the computation
        try {
            // Wait for the computation to terminate
            jssc.awaitTermination();
            
        } catch (InterruptedException e) {
            e.printStackTrace();
        } 

    }
}
