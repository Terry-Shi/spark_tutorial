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
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.rdd.RDD;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

import scala.Tuple2;
import scala.collection.Iterator;


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
        
    public static void main1(String[] args) {
    	long begin = System.currentTimeMillis();
        SparkConf sparkConf = new SparkConf().setAppName("Bayes").setMaster("local[4]");
        SparkContext  sc = new SparkContext (sparkConf);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc); //new JavaSparkContext(sparkConf);
        
        // Load from collection 
        // jsc.parallelize(List);
        // Load from file
        JavaRDD<String> docs = jsc.textFile("data/svd/reuters21578/news.txt"); // Load documents (one news per line).
        JavaRDD<Iterable<String>> dataset = docs.map(new Function<String, Iterable<String>>(){
            @Override
            public Iterable<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" "));
            }
        });
        
        // TF-IDF 
        // Term Frequency (tf)：即此Term 在此文档中出现了多少次。tf 越大说明越重要。
        // Document Frequency (df)：即有多少文档包含次Term。df 越大说明越不重要。
        HashingTF tf = new HashingTF();
        JavaRDD<Vector> tfResult = tf.transform(dataset).cache();
        IDF idf = new IDF();
        IDFModel idfModel = idf.fit(tfResult);
        JavaRDD<Vector> tfIdfResult = idfModel.transform(tfResult);
        long endOfTFIDF = System.currentTimeMillis();
        System.out.println("*********" + (endOfTFIDF - begin) + "***********");
        // Create a RowMatrix from JavaRDD<Vector>.
        RowMatrix mat = new RowMatrix(tfIdfResult.rdd());

        // Compute the top 20 singular values and corresponding singular vectors.
        // 第一个参数20意味着取top 20个奇异值，第二个参数true意味着计算矩阵U，第三个参数意味小于1.0E-9d的奇异值将被抛弃
        SingularValueDecomposition<RowMatrix, Matrix> svd = mat.computeSVD(20, true, 1.0E-9d);// TODO: meaning of 3rd argument
        // A = U * s *V
        RowMatrix U = svd.U(); //矩阵U
        Vector s = svd.s(); //奇异值
        Matrix V = svd.V(); //矩阵V 
       
        long endOfSVD = System.currentTimeMillis();
        System.out.println("*********" + (endOfSVD - endOfTFIDF) + "***********");
        
        System.out.println(U); // 20 cols, 925 rows
        RDD<Vector> vec = U.rows();
        Iterator<Vector> it = vec.toLocalIterator();
        int idx = 0;
        while (it.hasNext()) {
        	Vector tmp = it.next();
        	double[] d = tmp.toArray();
        	int biggestIdx = biggestIdx(tmp);
        	System.out.println(idx++ + " " + biggestIdx + " " + d[biggestIdx]);
        }
        
        System.out.println("-------------------");
    	System.out.println(s);
        System.out.println("-------------------");
        //System.out.println(V.numRows()); // 20 cols, 1048576 rows
        
        //getVectorforWord();
    }
    
    public static int biggestIdx(Vector vec){
    	double[] d = vec.toArray();
    	int biggestIdx = 0;
    	double currVal = d[0];
    	for (int i = 1; i < d.length; i++) {
			if (d[i] > currVal){
				currVal = d[i];
				biggestIdx = i;
			}
		}
    	return biggestIdx;
    }
    
//    public static void main(String[] args) {
//    	getNewsFile();
//	}
    
    /**
     * Get news from file. clean all the XML tags
     * 
     */
    public static void getNewsFile() {
    	String input = "data/svd/reuters21578/news.xml";
    	File inputFile = new File(input);
    	List<String> fileContent;
    	StringBuilder strB = new StringBuilder();
		try {
			fileContent = Files.readAllLines( Paths.get(input), Charset.forName("UTF-8"));
	    	for (String string : fileContent) {
				strB.append(string).append(" ");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
    	Document xmlDoc = Jsoup.parse(strB.toString(), "", Parser.xmlParser());
    	Elements elements = xmlDoc.select("BODY");
    	List<String> lines = new ArrayList<String>();
    	for (int i=0; i<elements.size(); i++){
    		lines.add(elements.get(i).text());
    	}
    	try {
			Files.write(Paths.get("data/svd/reuters21578/news.txt"), lines, Charset.forName("UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    
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
        word2vec.setVectorSize(101); // size of vector, default value = 100
        Word2VecModel model = word2vec.fit(dataset);
        
        // 输入计算距离的命令即可计算与每个词最接近的词 (0-1)。 TODO:"接近"的实际含义是->数学含义是余弦函数的值
        Tuple2<String, Object>[] synonyms = model.findSynonyms("Emma", 3); // 3 means how many words in the return value.
        for (int i = 0; i < synonyms.length; i++) {
            System.out.println(synonyms[i]._1 + " --- " + synonyms[i]._2);
        }
        Vector vecEmma = model.transform("Emma");
        System.out.println("Emma size = " + vecEmma.size());
        Vector vecHarriet = model.transform("Harriet");
        System.out.println("Harriet size = " + vecHarriet.size());
        //Vector sum = vecEmma + vecHarriet; 
        
        // TODO: why findSynonyms("apple") is not working.
    }
}
