package org;

import com.dxfeed.api.DXEndpoint;
import com.dxfeed.event.market.Trade;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.spark.sql.streaming.TR;
import scala.Serializable;
import scala.runtime.AbstractFunction1;

public class Application {
    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir", "/home/vstepanov/spark-sc");
        DXEndpoint.getInstance().connect("localhost:7099");
        SparkSession spark = SparkSession
            .builder()
            .master("local[2]")
            .appName("qds")
            .getOrCreate();

        Dataset<Row> tradeRows = spark.readStream()
            .format("qds")
            .option("symbol", "AAPL")
            .load();

        Dataset<TR> words = tradeRows.map(new F(), Encoders.bean(TR.class));
        words.writeStream()
            .format("console")
            .start()
            .awaitTermination();

//        Dataset<Row> lines = spark
//                .readStream()
//                .format("socket")
//                .option("host", "tosdev11")
//                .option("port", 9999)
//                .load();

        // Split the lines into words
//        Dataset<String> words = lines
//                .as(Encoders.STRING())
//                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
//
//        // Generate running word count
//        Dataset<Row> wordCounts = words.groupBy("value").count();
//
//        StreamingQuery query = wordCounts.writeStream()
//                .outputMode("complete")
//                .format("console")
//                .start();
//
//        query.awaitTermination();
    }

    public static class F extends AbstractFunction1<Row, TR> implements Serializable {
        @Override
        public TR apply(Row v1) {
            return new TR();
        }
    }
}
