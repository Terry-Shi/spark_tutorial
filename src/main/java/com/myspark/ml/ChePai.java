package com.myspark.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LassoModel;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.mllib.regression.LassoWithSGD;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

import com.myspark.NaiveBayesAlgorithm;

public class ChePai {
    // 拍卖时间,投放数量,最低成交价,平均成交价,第n位,最低成交价截至时间,投标人数,警示价格
    // 期望输出：什么时候 出什么价格？
    public static int IDX_TIME = 0;
    public static int IDX_CAR_COUNT = 1;
    public static int IDX_LOW_PRICE = 2;
    public static int IDX_AVG_PRICE = 3;
    public static int IDX_LAST_TIME = 4;
    public static int IDX_NO = 5;
    public static int IDX_PEOPLE_COUNT = 6;
    public static int IDX_WARNING_PRICE = 7;
    
    
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Bayes").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        JavaRDD<String> data = sc.textFile("data/naiveBayes/chepai.txt");
        // For Java 7 or below
        RDD<LabeledPoint> parsedData = data.map(new Function<String, LabeledPoint>() {
             public LabeledPoint call(String line) {
                 String[] cols = line.split(",");
                 
                 double[] values = new double[3];
                 values[0] = Double.valueOf(cols[IDX_CAR_COUNT]);
                 values[1] = Double.valueOf(cols[IDX_PEOPLE_COUNT]);
                 values[2] = Double.valueOf(cols[IDX_WARNING_PRICE]);
                 String[] lastTimeArray = cols[IDX_LAST_TIME].split(":");
                 //double lastTime = Double.valueOf(lastTimeArray[1] + "." + lastTimeArray[2]);
                 double lowPrice = Double.valueOf(cols[IDX_LOW_PRICE]);
                 
                 // LabeledPoint代表一条训练数据，即打过标签的数据
                 return new LabeledPoint(lowPrice, Vectors.dense(values));
             }
         }).rdd();
        
        // Building the model
        int numIterations = 25;
        final LinearRegressionModel model = LinearRegressionWithSGD.train(parsedData, numIterations); 
        //final LassoModel model = LassoWithSGD.train(parsedData, numIterations);
        
        // Evaluate model on training examples and compute training error
        JavaRDD<Tuple2<Double, Double>> valuesAndPreds = parsedData.toJavaRDD().map(
          new Function<LabeledPoint, Tuple2<Double, Double>>() {
            public Tuple2<Double, Double> call(LabeledPoint point) {
              double prediction = model.predict(point.features());
              System.out.println("Actual value=" + point.label() + " -> prediction = " + (int)prediction);
              return new Tuple2<Double, Double>(prediction, point.label());
            }
          }
        );
        double MSE = new JavaDoubleRDD(valuesAndPreds.map(
          new Function<Tuple2<Double, Double>, Object>() {
            public Object call(Tuple2<Double, Double> pair) {
              return Math.pow(pair._1() - pair._2(), 2.0);
            }
          }
        ).rdd()).mean();
        System.out.println("training Mean Squared Error = " + MSE);
    }
    
    public static void bayes() {

        
        SparkConf sparkConf = new SparkConf().setAppName("Bayes").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        JavaRDD<String> data = sc.textFile("data/naiveBayes/chepai.txt");
        // For Java 7 or below
        RDD<LabeledPoint> parsedData = data.map(new Function<String, LabeledPoint>() {
             public LabeledPoint call(String line) {
                 String[] cols = line.split(",");
                 
                 double[] values = new double[2];
                 values[0] = Double.valueOf(cols[IDX_CAR_COUNT]);
                 values[1] = Double.valueOf(cols[IDX_PEOPLE_COUNT]);
                 String[] lastTimeArray = cols[IDX_LAST_TIME].split(":");
                 //double lastTime = Double.valueOf(lastTimeArray[1] + "." + lastTimeArray[2]);
                 double lowPrice = Double.valueOf(cols[IDX_LOW_PRICE]);
                 
                 // LabeledPoint代表一条训练数据，即打过标签的数据
                 return new LabeledPoint(lowPrice, Vectors.dense(values));
             }
         }).rdd();
         //System.out.println(parsedData.toDebugString());
         final NaiveBayesModel model = NaiveBayes.train(parsedData, 1.0);
         
         NaiveBayesAlgorithm.evaluation(parsedData);
         //System.out.println("Prediction of (14.23,1.71,2.43,15.6,127,2.8,3.06,.28,2.29,5.64,1.04,3.92,1065):" + model.predict(Vectors.dense(new double[]{14.23,1.71,2.43,15.6,127,2.8,3.06,.28,2.29,5.64,1.04,3.92,1065})));

    }
}
