package org.example.reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class KafkaConsumer {

    private SparkSession spark;

    public KafkaConsumer(SparkSession spark) {
        this.spark = spark;
    }

    public Dataset<String> readFromKafka(String topic) {
        // Kafka에서 데이터를 읽어옴
        return spark
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", topic)
                .option("group.id", "ub_preprocessor")
//                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());
    }
}
