package com.myspark;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import scala.Tuple2;


/**
 * SVD Algorithm example
 * 分类的关键是计算相关性。我们首先对两个文本计算出它们的内容词，或者说实词的向量，然后求这两个向量的夹角。
 * 在文本分类中，另一种办法是利用矩阵运算中的奇异值分解（Singular Value Decomposition，简称 SVD)。
 * 首先，我们可以用一个大矩阵A来描述这一百万篇文章和五十万词的关联性。这个矩阵中，每一行对应一篇文章，每一列对应一个词。
 * 我们只要对关联矩阵A进行一次奇异值分解，我们就可以同时完成了近义词分类和文章的分类。（同时得到每类文章和每类词的相关性）。
 * @author shijie
 * @link 参考代码 http://blog.selfup.cn/1243.html
 * https://github.com/apache/spark/tree/master/examples/src/main/java/org/apache/spark/examples/mllib
 * @link http://lancezhange.com/blog/2014/10/10/word2vec/
 * @link Word2Vec原理 http://techblog.youdao.com/?p=915
 * @link http://spark.apache.org/docs/1.1.0/api/java/
 * TODO: 文本向量的获得
 * TODO: 调试SVD代码
 */
public class SVDAlgorithm {
        
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Bayes").setMaster("local");
        SparkContext  sc = new SparkContext (sparkConf);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc); //new JavaSparkContext(sparkConf);
        
        // 要求源数据为一篇文章一行
        JavaRDD<String> docs = jsc.textFile("data/svd/reurers21578/news.txt"); // Load documents (one per line).
        JavaRDD<Iterable<String>> dataset = docs.map(new Function<String, Iterable<String>>(){
            @Override
            public Iterable<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" "));
            }
        });
//	    JavaRDD<String> dataset = docs.flatMap(new FlatMapFunction<String, String>(){
//	      @Override
//	      public Iterable<String> call(String t) throws Exception {
//	          return Arrays.asList(t.split(" "));
//	      }
//	    });
        
        // TF-IDF 
//      Term Frequency (tf)：即此Term 在此文档中出现了多少次。tf 越大说明越重要。
//      Document Frequency (df)：即有多少文档包含次Term。df 越大说明越不重要。
        HashingTF tf = new HashingTF();
        JavaRDD<Vector> tfResult = tf.transform(dataset).cache();
        IDF idf = new IDF();
        IDFModel idfModel = idf.fit(tfResult);
        JavaRDD<Vector> tfIdfResult = idfModel.transform(tfResult);
        
        // Create a RowMatrix from JavaRDD<Vector>.
        RowMatrix mat = new RowMatrix(tfIdfResult.rdd());

        // Compute the top 4 singular values and corresponding singular vectors.
        SingularValueDecomposition<RowMatrix, Matrix> svd = mat.computeSVD(4, true, 1.0E-9d);// TODO: meaning of 3rd argument
        RowMatrix U = svd.U();
        Vector s = svd.s();
        Matrix V = svd.V();
        
        System.out.println(U);
        System.out.println("-------------------");
    	System.out.println(s);
        System.out.println("-------------------");
        System.out.println(V);
        
        //getVectorforWord();
    }
    
    public static void getNewsFile(String startStr, String endStr) {
    	String input = "svd/reuters21578/news.txt";
    	File inputFile = new File(input);
    	List<String> fileContent;
		try {
			fileContent = Files.readAllLines( Paths.get(input), Charset.forName("UTF-8"));
			boolean start = false;
	    	List<String> outputFileContent = new ArrayList<String>();
	    	String tmp = "";
	    	for (String line : fileContent) {
	    		if (!start) {
	    			int idx = line.indexOf(startStr);
		    		if ( idx >=0 ) {
		    			start = true;
		    			
					}
	    		} else {
	    			
	    		}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
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
