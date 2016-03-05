package com.myspark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.mllib.linalg.Vector;

import scala.Tuple2;

public class MyWord2vec {
	public static void main(String[] args) {
		getVectorforWord();
	}
	
	public static void getVectorforWord() {
        SparkConf sparkConf = new SparkConf().setAppName("Bayes").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile("data/svd/pg158.txt"); //sc.parallelize(Arrays.asList(new String[]{"China shanghai", "apple","IBM", "HP", "Microsoft"}));

        JavaRDD<Iterable<String>> dataset = lines.map(new Function<String, Iterable<String>>(){
            @Override
            public Iterable<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" "));
            }
        });
        System.out.println(dataset.collect());
        
        // 根据输入的词的集合计算出词与词直接的距离
        Word2Vec word2vec = new Word2Vec();
        Word2VecModel model = word2vec.fit(dataset);
        
        // 输入计算距离的命令即可计算与每个词最接近的词 (0-1)。 TODO:"接近"的实际含义是？数学含义是余弦函数的值
        Tuple2<String, Object>[] synonyms = model.findSynonyms("Emma", 3); // 3 means how many words in the return value.
        for (int i = 0; i < synonyms.length; i++) {
            System.out.println(synonyms[i]._1 + " --- " + synonyms[i]._2);
        }
        Vector vec = model.transform("Emma");
                
        // TODO: why findSynonyms("apple") is not working.
    }
}
