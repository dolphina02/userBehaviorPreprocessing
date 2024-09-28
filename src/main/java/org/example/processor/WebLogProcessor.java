package org.example.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.example.entity.WebLog;

public class WebLogProcessor {

    public Dataset<WebLog> cleanLogs(Dataset<WebLog> logs) {
        // WebLog 데이터 전처리 (예: 상태 코드 200 필터링)
        return logs.filter((FilterFunction<WebLog>) log -> log.getTimestamp() != null);
    }

    public Dataset<WebLog> mapToWebLog(Dataset<String> kafkaData) {
        // Kafka에서 받은 JSON 데이터를 WebLog 클래스로 변환
        return kafkaData.map((MapFunction<String, WebLog>) record -> {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(record, WebLog.class);
        }, Encoders.bean(WebLog.class));
    }
}