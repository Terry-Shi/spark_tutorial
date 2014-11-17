package com.myspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class LengthCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("data.txt");
        
//        {
//            JavaRDD<Integer> lineLengths = lines.map(s -> s.length()); // format is "input data" -> "output data" OR 形式参数->方法体
//            int totalLength = lineLengths.reduce((a, b) -> a + b);
//        }
        
        {
            JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
                public Integer call(String s) { return s.length(); }
            });
            int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() { // T1, T2 and R 
                public Integer call(Integer a, Integer b) { return a + b; }
            });
        }
        
        {
            // define function class first
            JavaRDD<Integer> lineLengths = lines.map(new GetLength());
            int totalLength = lineLengths.reduce(new Sum());
        }
    }
}

class GetLength implements Function<String, Integer> {
    public Integer call(String s) {
        return s.length();
    }
}

class Sum implements Function2<Integer, Integer, Integer> {
    public Integer call(Integer a, Integer b) {
        return a + b;
    }
}

  