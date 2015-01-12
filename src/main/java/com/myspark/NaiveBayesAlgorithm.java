package com.myspark;

import java.io.File;
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

import scala.Tuple2;

/**
 * Ref:http://blog.selfup.cn/683.html
 * @author shijie
 * TODO: Additive smoothing的值为1.0　含义？
 */
public class NaiveBayesAlgorithm {
    
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Bayes").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        // TODO: 输入数据往往不是数字形式的需要 类型转换  String -> Double.
        // Question1: 怎么保证一一对应？
        // 属性1：发件人邮件地址 aaa@hp.com  hashcode() ? MD5 ? or sequence number ? 优点：多次计算结果一样。缺点：不能保证不重复。
        // 属性2：Topic ID
        // 属性3:To OR Cc
        // Label: reply OR forward OR Delete
        // Question2: 怎么保存?  Map? embedded Database?
       
        JavaRDD<String> data = sc.textFile("data/naiveBayes/wine.data.txt");
        NaiveBayesModel model = createModelWithWineData(data);
        
        //evaluation(formatWineData(data));
        
        // NaiveBayesModel model = createModelWithDataTxt(sc, sc.textFile("data/naiveBayes/data.txt"));
        
        // Load from list
//        List<LabeledPoint> inputData = new ArrayList<LabeledPoint>();
//        inputData.add(new LabeledPoint(0.0 ,Vectors.dense(new double[]{1, 0, 0})));
//        inputData.add(new LabeledPoint(0.0 ,Vectors.dense(new double[]{2, 0, 0})));
//        inputData.add(new LabeledPoint(0.0 ,Vectors.dense(new double[]{1, 0, 0.1})));
//        inputData.add(new LabeledPoint(0.0 ,Vectors.dense(new double[]{2, 0, 0.2})));
//        JavaRDD<LabeledPoint> parsedData = sc.parallelize(inputData);
        
        // 训练模型， Additive smoothing的值为1.0（默认值）
        //final NaiveBayesModel model = NaiveBayes.train(data, 1.0);
        saveModelFile(sc,model, "data/naiveBayes/model", true);
        
        //NaiveBayesModel model = loadModelFile(sc, "data/naiveBayes/model");
    
        //predict(model);
    }
    
    
    /**
     * Input file format {Label,AttributeA's value AttributeB's value AttributeC's value}
     * data/naiveBayes/data.txt
     * // 0,1 0 0
     * // 0,2 0 0
     * // 0,1 0 0.1
     * @param sc
     * @param path
     * @return Model
     */
    public static NaiveBayesModel createModelWithDataTxt(JavaSparkContext sc, String path) {
        JavaRDD<String> data = sc.textFile(path);
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
        final NaiveBayesModel model = NaiveBayes.train(parsedData, 1.0);
        return model;
    }
    
    
    
    /**
     * wine.data.txt
     * @param sc
     * @param path
     * @return
     */
    public static NaiveBayesModel createModelWithWineData(JavaRDD<String> data) {
        RDD<LabeledPoint> parsedData = formatWineData(data);
        
        final NaiveBayesModel model = NaiveBayes.train(parsedData, 1.0);
        return model;
    }
    
    private static RDD<LabeledPoint> formatWineData(JavaRDD<String> data){
        RDD<LabeledPoint> parsedData = data.map(new Function<String, LabeledPoint>() {
            public LabeledPoint call(String line) {
                String[] parts = line.split(",");
                
                double[] values = new double[parts.length -1];
                for (int i = 1; i < parts.length; i++) {
                    values[i-1] =  Double.valueOf(parts[i]);
                }
                // LabeledPoint代表一条训练数据，即打过标签的数据
                return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(values));
            }
        }).rdd();
        
        return parsedData;
    }
    
    
//    public static void saveModelFile(JavaSparkContext sc, String path, NaiveBayesModel model, boolean forceOverwrite) {
//        JavaRDD<NaiveBayesModel> persistModel = sc.parallelize(Arrays.asList(new NaiveBayesModel[]{model}));
//        persistModel.saveAsObjectFile(path);
//    }
    
    public static void saveModelFile(JavaSparkContext sc, NaiveBayesModel model, String modelOutputPath, boolean forceOverwrite) {
        if (forceOverwrite) {
            deleteDir(new File(modelOutputPath));
        }
        
        JavaRDD<NaiveBayesModel> persistModel = sc.parallelize(Arrays.asList(new NaiveBayesModel[]{model}));
        persistModel.saveAsObjectFile(modelOutputPath);
    }
    
    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            //递归删除目录中的子目录下
            for (int i=0; i< children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }
    
    public static NaiveBayesModel loadModelFile(JavaSparkContext sc, String path) {
        JavaRDD<NaiveBayesModel> persistModel = sc.objectFile(path);
        return persistModel.first();
    }
    
    public static void predict(NaiveBayesModel model) {
        // 预测类别 for data.txt
//        System.out.println("Prediction of (0.5, 3.0, 0.5):" + model.predict(Vectors.dense(new double[]{0.5, 3.0, 0.5})));
//        System.out.println("Prediction of (1.5, 0.4, 0.6):" + model.predict(Vectors.dense(new double[]{1.5, 0.4, 0.6})));
//        System.out.println("Prediction of (0.3, 0.4, 2.6):" + model.predict(Vectors.dense(new double[]{0.3, 0.4, 2.6})));
       
        // 预测类别 for wine.data.txt
        System.out.println("Prediction of (14.23,1.71,2.43,15.6,127,2.8,3.06,.28,2.29,5.64,1.04,3.92,1065):" + model.predict(Vectors.dense(new double[]{14.23,1.71,2.43,15.6,127,2.8,3.06,.28,2.29,5.64,1.04,3.92,1065})));
    }
    
    /**
     * 用测试数据来验证模型的精度
     * @param parsedData test data
     * @return evaluation result
     */
    public static double evaluation(RDD<LabeledPoint> parsedData) {
                
        //分隔为两个部分，70%的数据用于训练，30%的用于测试
        RDD<LabeledPoint>[] splits = parsedData.randomSplit(new double[]{0.7, 0.3}, 11L);
        JavaRDD<LabeledPoint> training = splits[0].toJavaRDD();
        JavaRDD<LabeledPoint> test = splits[1].toJavaRDD();
        
        final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0); 
        JavaRDD<Double> prediction = test.map(new Function<LabeledPoint, Double>() {
            @Override
            public Double call(LabeledPoint v1) throws Exception {
                return model.predict(v1.features());
            }
        });
        System.out.println("JavaRDD<Double> prediction = " + prediction.toArray().toString());
        
        // TODO: http://www.adamcrume.com/blog/archive/2014/02/19/fixing-sparks-rdd-zip
        JavaPairRDD<Double, LabeledPoint> predictionAndLabel = prediction.zip(test);
        long matchedNumber = predictionAndLabel.filter(new Function<Tuple2<Double, LabeledPoint>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Double, LabeledPoint> v1) throws Exception {
                return v1._1.equals(v1._2.label());
            }
        }).count();

        double accuracy = 1.0 * matchedNumber / test.count();
        System.out.println("Matched data Number = " + matchedNumber);
        System.out.println("Total test data is = " + test.count());
        System.out.println("Accuracy=" + accuracy);
        return accuracy;
    }
}

