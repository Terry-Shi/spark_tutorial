package com.myspark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;

/**
 * ref:http://blog.selfup.cn/683.html
 * @author shijie
 *
 */
public class NaiveBayesAlgorithm {
//    public static void main(String[] args) {
//    JavaRDD<LabeledPoint> training = ... // training set
//    JavaRDD<LabeledPoint> test = ... // test set
//
//            final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);
//
//            JavaPairRDD<Double, Double> predictionAndLabel = 
//              test.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
//                @Override public Tuple2<Double, Double> call(LabeledPoint p) {
//                  return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
//                }
//              });
//            double accuracy = 1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
//                @Override public Boolean call(Tuple2<Double, Double> pl) {
//                  return pl._1() == pl._2();
//                }
//              }).count() / test.count();
//    }
    
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Bayes").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        // TODO: 输入数据往往不是数字形式的需要转换  String -> Double.
        // Question1: 怎么保证一一对应？
        // 属性1：发件人邮件地址 aaa@hp.com  hashcode() ? MD5 ? or sequence number ?
        // 属性2：topic ID
        // 属性3: To OR Cc
        // Label: reply OR forward OR Delete
        // Question2: 怎么保存? Use simple Map ?  Use in-memory Database H2 ?
        
        
        
        JavaRDD<String> data = sc.textFile("data/naiveBayes/data.txt");
    // For Java 8 lambda 
    //    RDD<LabeledPoint> parsedData = data.map(line -> {
    //        String[] parts = line.split(",");
    //        double[] values = Arrays.stream(parts[1].split(" "))
    //              .mapToDouble(Double::parseDouble)
    //              .toArray();
    //        // LabeledPoint代表一条训练数据，即打过标签的数据
    //        return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(values));
    //    }).rdd();
        
        // For Java 7 or below
        RDD<LabeledPoint> parsedData = data.map(new Function<String, LabeledPoint>() {
            public LabeledPoint call(String line) {
                String[] parts = line.split(",");
                String[] sarray = parts[1].split(" ");
                double[] values = new double[sarray.length];
                for (int i = 0; i < values.length; i++) {
                    values[i] =  Double.valueOf(sarray[i]);
                }
                // LabeledPoint代表一条训练数据，即打过标签的数据
                return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(values));
            }
        }).rdd();
        
//        0,1 0 0
//        0,2 0 0
//        0,1 0 0.1
//        0,2 0 0.2
//        0,1 0.1 0
//        0,2 0.2 0
//        1,0 1 0.1
//        1,0 2 0.2
//        1,0.1 1 0
//        1,0.2 2 0
//        1,0 1 0
//        1,0 2 0
//        2,0.1 0 1
//        2,0.2 0 2
//        2,0 0.1 1
//        2,0 0.2 2
//        2,0 0 1
//        2,0 0 2        
        // Load from list
//        List<LabeledPoint> inputData = new ArrayList<LabeledPoint>();
//        inputData.add(new LabeledPoint(0.0 ,Vectors.dense(new double[]{1, 0, 0})));
//        inputData.add(new LabeledPoint(0.0 ,Vectors.dense(new double[]{2, 0, 0})));
//        inputData.add(new LabeledPoint(0.0 ,Vectors.dense(new double[]{1, 0, 0.1})));
//        inputData.add(new LabeledPoint(0.0 ,Vectors.dense(new double[]{2, 0, 0.2})));
//        JavaRDD<LabeledPoint> parsedData = sc.parallelize(inputData);
        
        //训练模型， Additive smoothing的值为1.0（默认值）
//        final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0); // TODO: how to save model, then re-use it.
//        saveModelFile(sc, "data/naiveBayes/model", model);
        
        NaiveBayesModel model = loadModelFile(sc, "data/naiveBayes/model");
    
        predict(model);
    }
    
    public static void saveModelFile(JavaSparkContext sc, String path, NaiveBayesModel model) {
        JavaRDD<NaiveBayesModel> persistModel = sc.parallelize(Arrays.asList(new NaiveBayesModel[]{model}));
        persistModel.saveAsObjectFile(path);
    }
    
    public static NaiveBayesModel loadModelFile(JavaSparkContext sc, String path) {
        JavaRDD<NaiveBayesModel> persistModel = sc.objectFile(path);
        return persistModel.first();
    }
    
    public static void predict(NaiveBayesModel model) {
        //预测类别
        System.out.println("Prediction of (0.5, 3.0, 0.5):" + model.predict(Vectors.dense(new double[]{0.5, 3.0, 0.5})));
        System.out.println("Prediction of (1.5, 0.4, 0.6):" + model.predict(Vectors.dense(new double[]{1.5, 0.4, 0.6})));
        System.out.println("Prediction of (0.3, 0.4, 2.6):" + model.predict(Vectors.dense(new double[]{0.3, 0.4, 2.6})));
    }
    
    public static double evaluation(RDD<LabeledPoint> parsedData) {
        //分隔为两个部分，70%的数据用于训练，30%的用于测试
        RDD<LabeledPoint>[] splits = parsedData.randomSplit(new double[]{0.7, 0.3}, 11L);
        JavaRDD<LabeledPoint> training = splits[0].toJavaRDD();
        JavaRDD<LabeledPoint> test = splits[1].toJavaRDD();
        
        //    JavaRDD<Double> prediction = test.map(p -> model.predict(p.features()));
        //    JavaPairRDD<Double, Double> predictionAndLabel = prediction.zip(test.map(LabeledPoint::label));
        //    //用测试数据来验证模型的精度
        //    double accuracy = 1.0 * predictionAndLabel.filter(pl -> pl._1().equals(pl._2())).count() / test.count();
        //    System.out.println("Accuracy=" + accuracy);
        return accuracy;
    }
}

