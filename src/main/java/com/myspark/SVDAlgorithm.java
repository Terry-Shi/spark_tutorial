package com.myspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;


/**
 * SVD Algorithm example
 * 分类的关键是计算相关性。我们首先对两个文本计算出它们的内容词，或者说实词的向量，然后求这两个向量的夹角。
 * 在文本分类中，另一种办法是利用矩阵运算中的奇异值分解（Singular Value Decomposition，简称 SVD)。
 * 首先，我们可以用一个大矩阵A来描述这一百万篇文章和五十万词的关联性。这个矩阵中，每一行对应一篇文章，每一列对应一个词。
 * 我们只要对关联矩阵A进行一次奇异值分解，我们就可以同时完成了近义词分类和文章的分类。（同时得到每类文章和每类词的相关性）。
 * @author shijie
 * @link http://blog.selfup.cn/1243.html
 * TODO: 文本向量的获得
 * TODO: 调试SVD代码
 */
public class SVDAlgorithm {
    
    
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Bayes").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        JavaRDD<String> lines = sc.textFile("data.txt");
        // TF-IDF 
//        Term Frequency (tf)：即此Term 在此文档中出现了多少次。tf 越大说明越重要。
//        Document Frequency (df)：即有多少文档包含次Term。df 越大说明越不重要  。
        lines.map(f);
        HashingTF hashingTF = new HashingTF();
        hashingTF.transform(dataset);
        

        IDF idf = new IDF();
    }
    
}
