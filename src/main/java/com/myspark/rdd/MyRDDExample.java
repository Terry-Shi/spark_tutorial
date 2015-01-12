package com.myspark.rdd;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;

import com.myspark.util.FileUtil;


/**
 * 实践RDD中的各种操作。
 * Ref: https://www.zybuluo.com/jewes/note/35032
 * @author shijie
 *
 */
public class MyRDDExample {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\\\temp\\winutil\\");
        
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // map(sc);
        // flatMap(sc);
        //mapToPair(sc);
//        reduceByKey(sc);
        reduce(sc);
        
        sc.close();
    }

    /**
     * map是对RDD中的每个元素都执行一个指定的函数来产生一个新的RDD。任何原RDD中的元素在新RDD中都有且只有一个元素与之对应。
     * 
     * 新旧RDD中元素数量一致。 且一一对应。
     * @param sc
     */
    public static void map(JavaSparkContext sc) {
        JavaRDD<String> oldRDD = sc.textFile("data/rdd/flatMap.txt").cache();
        
        JavaRDD<Integer> newRDD = oldRDD.map(new Function<String, Integer>() { // 第一个类型String对应call的输入参数类型，第二个类型Integer对应call的返回值类型
            @Override
            public Integer call(String v1) throws Exception {
                return v1.length();
            }});
        
        FileUtil.deleteDir( new File("data/rdd/map_output") );
        
        newRDD.saveAsTextFile("data/rdd/map_output");
    }
    
    /**
     * flatMap与map类似，区别是原RDD中的元素经map处理后只能生成一个元素，而原RDD中的元素经flatmap处理后可生成多个元素来构建新RDD。 
     * 举例：对原RDD中的每个元素产生y个元素. 新旧RDD中元素为1对多关系。
     * 
     * 将原RDD中每一行的字符串按空格分隔,新RDD中的一个元素为一个单词
     */
    public static void flatMap(JavaSparkContext sc) {
        JavaRDD<String> oldRDD = sc.textFile("data/rdd/flatMap.txt").cache();
        
        JavaRDD<String> newRDD = oldRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" "));
            }
        } );
        
        //System.out.print(newRDD.toDebugString());
        FileUtil.deleteDir( new File("data/rdd/flatMap_output") );
        newRDD.saveAsTextFile("data/rdd/flatMap_output");
    }
    
    /**
     * 新旧RDD中元素个数相同，把旧RDD中的单一元素转换为新RDD中的KeyValue元素结构。
     * 
     * old_value -> key, new_value
     */
    public static void mapToPair(JavaSparkContext sc) {
        JavaRDD<String> oldRDD = sc.textFile("data/rdd/flatMap.txt").cache();
        
        JavaPairRDD<String, Integer> newRDD = oldRDD.mapToPair(new PairFunction<String, String, Integer>() { // （String call()的输入参数类型，String， Integer call()返回值类型）
            @Override
            public Tuple2<String, Integer> call(String t) throws Exception {
                return new Tuple2<String, Integer>(t, 1);
            }
            
        }); 
        
        //newRDD.toDebugString();
        FileUtil.deleteDir( new File("data/rdd/mapToPair") );
        newRDD.saveAsTextFile("data/rdd/mapToPair");
    }
    
    
    /**
     * reduce将RDD中元素两两传递给输入函数，同时产生一个新的值，新产生的值与RDD中下一个元素再被传递给输入函数直到最后只有一个值为止。
     * @param sc
     */
    public static void reduce(JavaSparkContext sc) {
//        List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
//        list.add(new Tuple2("word1", 1));
//        list.add(new Tuple2("word2", 1));
//        list.add(new Tuple2("word3", 1));
//        list.add(new Tuple2("word3", 1));
//        
//        JavaPairRDD<String, Integer> oldRDD = sc.parallelizePairs(list);
        
        JavaRDD<Integer> oldRDD = sc.parallelize(Arrays.asList(1, 2, 1, 1));
        
        Integer result = oldRDD.reduce(new Function2<Integer, Integer, Integer>(){
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
            
        } );
        
        System.out.println("reduce result = " + result);
    }
    
    /**
     * reduceByKey就是对元素为KV对的RDD中Key相同的元素的Value进行reduce，因此，Key相同的多个元素的值被reduce为一个值，然后与原RDD中的Key组成一个新的KV对。
     * @param sc
     */
    public static void reduceByKey(JavaSparkContext sc) {
        JavaRDD<String> oldRDD = sc.textFile("data/rdd/flatMap.txt").cache();
        
         // Split each line into words
        JavaRDD<String> words = oldRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String x) {
                return Arrays.asList(x.split(" "));
            }
        });

        // Count each word in each batch
        JavaPairRDD<String, Integer> pairs = words
                .mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                });
        
        
        JavaPairRDD<String, Integer> wordCounts = pairs
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) throws Exception {
                        return i1 + i2;
                    }
                });
        
        
        FileUtil.deleteDir( new File("data/rdd/reduceByKey") );
        wordCounts.saveAsTextFile("data/rdd/reduceByKey");
    }
    
}
