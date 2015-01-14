package com.myspark.sharedvariables;

import java.util.Arrays;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

/**
 * 
 * @author shijie
 *
 */
public class SharedVariables {
    public static void main(String[] args) {
        
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Shared Variables");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        
        sc.close();
    }
    
    /**
     * 主要用于共享分布式计算过程中各个 task 都会用到的只读变量，broadcast 变量只会在每台计算机器上保存一份，而不会每个task都传递一份，节省空间，效率也高。Spark 的HadoopRDD 的 实现 中，就采用 broadcast 进行 Hadoop JobConf 的传输。官方文档的说法 是当task的大小大于20k时，就可以考虑用 broadcast 进行优化
     * @param sc
     */
    public static void broadcastVariables(JavaSparkContext sc) {
        Broadcast<int[]> broadcastVar = sc.broadcast(new int[] {1, 2, 3});
        broadcastVar.value();
    }
    
    /**
     * accumulator 支持在 worker 端进行对其累加操作 +=，但不能读取数据，类似 hadoop 中的counter，但其除了支持 Int 和 Double 类型外，用户还可以自定义类型，只要该类实现相应接口即可。此外，它还支持 type A += type B，即累加结果的类型和累加的值的类型无须一致。
     * @param sc
     */
    public static void accumulators(JavaSparkContext sc) {
        final Accumulator<Integer> accum = sc.accumulator(0);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        
        rdd.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Integer t) throws Exception {
                accum.add(t);
            }
            
        });
        //rdd.foreach(x -> accum.add(x));
        // ...
        // 10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s

        accum.value();
        // returns 10
    }
    
}
