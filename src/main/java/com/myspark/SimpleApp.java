package com.myspark;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;


/**
 * Word Count
 * @author shijie
 *
 */
public class SimpleApp {
    public static void main(String[] args) {
        String logFile = "README.md"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache(); // QUESTION: How many RDD operations ? what's the difference of using cache() ?

        System.out.println("Total lines: " + logData.count()); // exclude the last empty line
        
        // RDD operation: RDD.filter(Function) ?   Input parameter is one line from RDD file.
        // Return value of filter() method ? a RDD file which contains boolean value
        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("a");
            }
        }).count(); // count() is the totally number of lines

        // save the line which contains "a"
        logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("a");
            }
        }).saveAsTextFile("data/fileContainsA.txt");
        
        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("b");
            }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    }
}
