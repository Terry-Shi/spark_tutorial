package com.myspark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class BroadcastVariables {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Broadcast Variables Example");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        Broadcast<int[]> broadcastVar = sc.broadcast(new int[] {1, 2, 3});

        int[] ret = broadcastVar.value();
        System.out.println((Arrays.asList(ret)).toString());
        // returns [1, 2, 3]
    }

}
