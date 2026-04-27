package com.invincibleagam.models;

public class ParsedLog {
    public String host;
    public String timestamp;
    public String logDate;
    public int logHour;
    public String httpMethod;
    public String resourcePath;
    public String protocolVersion;
    public int statusCode;
    public long bytesTransferred;

    public ParsedLog(String host, String timestamp, String logDate, int logHour, String httpMethod,
                     String resourcePath, String protocolVersion, int statusCode, long bytesTransferred) {
        this.host = host;
        this.timestamp = timestamp;
        this.logDate = logDate;
        this.logHour = logHour;
        this.httpMethod = httpMethod;
        this.resourcePath = resourcePath;
        this.protocolVersion = protocolVersion;
        this.statusCode = statusCode;
        this.bytesTransferred = bytesTransferred;
    }
}
