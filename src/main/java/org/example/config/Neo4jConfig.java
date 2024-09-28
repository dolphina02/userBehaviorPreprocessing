package org.example.config;

import org.apache.spark.sql.SparkSession;

public class Neo4jConfig {

    public SparkSession getSparkSession() {
        return SparkSession.builder()
                .appName("user behavior analysis with WebLog")
                .master("local[*]")  // 로컬 모드로 실행
                .config("spark.sql.debug.maxToStringFields", "1000")
                .config("spark.neo4j.bolt.url", "bolt://localhost:7687")
                .config("spark.neo4j.bolt.user", "neo4j")
                .config("spark.neo4j.bolt.password", "neo4jpassword")
                .getOrCreate();
    }
}
