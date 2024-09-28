package org.example.job;

//TIP 코드를 <b>실행</b>하려면 <shortcut actionId="Run"/>을(를) 누르거나
// 에디터 여백에 있는 <icon src="AllIcons.Actions.Execute"/> 아이콘을 클릭하세요.
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.config.Neo4jConfig;
import org.example.session.SessionManager;
import org.example.entity.WebLog;
import org.example.processor.WebLogProcessor;
import org.example.reader.KafkaConsumer;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;

public class UbLogPreprocessingJob {

    public static void main(String[] args) {

        // 1. SessionManager에서 SparkSession 생성
        SessionManager sessionManager = new SessionManager();
        SparkSession spark = sessionManager.getSparkSession();  // SessionManager에서 SparkSession 가져오기

        // 2. Neo4j 설정을 기존 SparkSession에 추가
        spark.conf().set("spark.neo4j.bolt.url", "bolt://localhost:7687");  // Neo4j Bolt URL
        spark.conf().set("spark.neo4j.bolt.user", "neo4j");  // Neo4j 사용자
        spark.conf().set("spark.neo4j.bolt.password", "password");  // Neo4j 비밀번호


        // 3. Kafka에서 데이터 읽기
        KafkaConsumer kafkaConsumer = new KafkaConsumer(spark);
        Dataset<String> kafkaData = kafkaConsumer.readFromKafka("ublog");

        // 4. WebLog 데이터 처리
        WebLogProcessor webLogProcessor = new WebLogProcessor();
        Dataset<WebLog> logs = webLogProcessor.mapToWebLog(kafkaData);
        Dataset<WebLog> cleanLogs = webLogProcessor.cleanLogs(logs);

        // 4. 변환된 데이터를 Neo4j에 전송 (Kafka 데이터를 그대로 저장)
        // 4. 사용자와 URL 노드를 생성하고 관계를 형성
        logs.write()
                .format("org.neo4j.spark.DataSource")
                .mode("Overwrite")
                .option("url", "bolt://localhost:7687")  // Neo4j Bolt URL 추가
                .option("authentication.basic.username", "neo4j")  // Neo4j 사용자명
                .option("authentication.basic.password", "neo4jpassword")  // Neo4j 비밀번호
                // User와 URL 노드를 생성하고 관계 형성
                .option("query", "MERGE (u:User {userId: event.userId}) "
                        + "MERGE (url:URL {address: event.url}) "
                        + "MERGE (u)-[:VISITED {timestamp: event.timestamp}]->(url)")  // 사용자와 URL을 연결
                .save();


        // 5. 세션 할당 및 사용자 여정 분석
        Dataset<Row> sessionizedLogs = sessionManager.assignSession(cleanLogs);
        Dataset<Row> userJourneys = sessionManager.analyzeUserJourney(sessionizedLogs);

        // 6. 배열을 문자열로 변환 (구분자로 ',' 사용)
        Dataset<Row> transformedUserJourneys = userJourneys
                .withColumn("url_path", concat_ws(",", col("url_path")));  // 배열을 문자열로 변환

        // 7. 변환된 데이터를 CSV로 저장
        transformedUserJourneys.write()
                .option("header", "true")  // CSV 헤더 추가
                .mode("overwrite")
                .csv("/Users/jdaddy/sampleData");  // 저장 경로 지정


        // 9. Spark 세션 종료
        spark.stop();

    }
}
