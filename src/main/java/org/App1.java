package org;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App1 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("hellowolrd").getOrCreate();
        Dataset<Row> lines = spark.read().text("/home/vasste/madeline.phar.version");
        long cntA = lines.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row value) throws Exception {
                return value.mkString();
            }
        }, Encoders.STRING()).filter((FilterFunction<String>) value -> value.contains("a")).count();

        System.out.println(cntA);

        spark.stop();
    }
}