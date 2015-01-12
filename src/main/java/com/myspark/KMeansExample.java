package com.myspark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.SparkConf;

public class KMeansExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("K-means Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load and parse data
        String path = "data/kmeans/kmeans_data.txt";
        JavaRDD<String> data = sc.textFile(path);
        JavaRDD<Vector> parsedData = data.map(
          new Function<String, Vector>() {
            public Vector call(String s) {
              String[] sarray = s.split(" ");
              double[] values = new double[sarray.length-1];
              for (int i = 0; i < sarray.length-1; i++)
                values[i] = Double.parseDouble(sarray[i+1]);
              return Vectors.dense(values);
            }
          }
        );

        // Cluster the data into two classes using KMeans
        int numClusters = 3;
        int numIterations = 10;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
        
        Vector v =  Vectors.dense(new double[]{1, 1, 0.5});
        int i = clusters.predict(v);
        System.out.println("China football is Level " +  i);
        
        System.out.println("Cluster centers:");
        for (Vector center : clusters.clusterCenters()) {
          System.out.println(" " + center);
        }
        
        JavaRDD<Integer> results = clusters.predict(parsedData);
        System.out.println(results.toArray());
        
        sc.stop();
      }
}
