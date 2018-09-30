package com.myspark.streaming;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

/**
 * ref: https://dongkelun.com/2018/05/17/sparkKafka/
 * https://forum.huawei.com/enterprise/zh/thread-451863-1-1.html
 * @author xzy
 *
 */
public class SparkStreamingWithKafkaExample {
    
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000)); //Durations.seconds(1)
        
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "earliest"); //latest
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("topicA", "topicB");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
          KafkaUtils.createDirectStream(
            jssc,
            LocationStrategies.PreferConsistent(), // This will distribute partitions evenly across available executors. 
            ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams) // subscribe to a fixed collection of topic
          );

        JavaPairDStream<String, String> jpds = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
        
        jpds.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
            @Override
            public void call(JavaPairRDD<String, String> rdd) throws Exception {
                rdd.foreachPartition(it -> {
                    while (it.hasNext()) {
                        Tuple2<String, String> pair = it.next();
                        System.out.println(pair._1 + pair._2);
                    }
                });
            }
        });
        
        jssc.start(); 
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            jssc.stop();
        }
    }
    
}
