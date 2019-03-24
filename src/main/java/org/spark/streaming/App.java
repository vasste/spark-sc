package org.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class App {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("app2").setMaster("local[2]");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Duration.apply(10000));
        JavaDStream<String> customReceiverStream = ssc.receiverStream(new JavaCustomReceiver("localhost", 9999));
        customReceiverStream.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
            @Override
            public void call(JavaRDD<String> v1, Time v2) throws Exception {
                System.out.println(v1);
            }
        });
        ssc.start();
        ssc.awaitTermination();
    }
}
