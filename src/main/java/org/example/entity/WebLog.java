package org.example.entity;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class WebLog {
    private String ipAddress;
    private String url;
    private Timestamp timestamp;
    private String userId;
    private String sessionId;
}
