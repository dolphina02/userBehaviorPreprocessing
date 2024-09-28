package org.example.session;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.example.entity.WebLog;

import static org.apache.spark.sql.functions.*;

public class SessionManager {

    private SparkSession spark;

    // 생성자를 통해 SparkSession을 생성 및 초기화
    public SessionManager() {
        this.spark = SparkSession.builder()
                .appName("user behavior analysis with WebLog")
                .master("local[*]")  // 로컬 모드로 실행
                .config("spark.sql.debug.maxToStringFields", "1000")
                .getOrCreate();
    }

    // SparkSession을 반환하는 메서드
    public SparkSession getSparkSession() {
        return this.spark;
    }

    public Dataset<Row> assignSession(Dataset<WebLog> cleanLogs) {
        // 사용자별 세션을 정의하기 위해 이전 타임스탬프 계산
        WindowSpec windowSpec = Window.partitionBy("userId").orderBy("timestamp");

        Dataset<Row> logsWithSession = cleanLogs.withColumn("prev_timestamp", lag("timestamp", 1).over(windowSpec))
                .withColumn("session_flag", when(col("prev_timestamp").isNull()
                        .or(expr("unix_timestamp(timestamp) - unix_timestamp(prev_timestamp) > 1800")), 1)
                        .otherwise(0));

        // 세션 플래그의 누적 합계를 사용하여 세션 ID 부여
        return logsWithSession.withColumn("sessionId", sum("session_flag").over(windowSpec));
    }

    public Dataset<Row> analyzeUserJourney(Dataset<Row> sessionizedLogs) {
        // 사용자별 및 세션별로 URL 경로를 추적
        return sessionizedLogs.groupBy("userId", "sessionId")
                .agg(collect_list("url").alias("url_path"));
    }
}
