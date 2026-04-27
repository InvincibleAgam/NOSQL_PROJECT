package com.invincibleagam.core;

import com.invincibleagam.models.ParsedLog;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;
import java.util.Locale;

public class LogParser {
    // 199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
    private static final Pattern LOG_PATTERN = Pattern.compile("^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[([^\\]]+)\\]\\s+\"([^\"]*)\"\\s+(\\d{3})\\s+(\\S+)$");
    private static final DateTimeFormatter INPUT_FORMATTER = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);

    public static ParsedLog parse(String line) {
        Matcher m = LOG_PATTERN.matcher(line);
        if (!m.matches()) {
            return null;
        }

        try {
            String host = m.group(1);
            String timestampStr = m.group(2);
            String request = m.group(3);
            int statusCode = Integer.parseInt(m.group(4));
            String bytesStr = m.group(5);

            // Parse timestamp
            LocalDateTime dateTime = LocalDateTime.parse(timestampStr, INPUT_FORMATTER);
            String logDate = String.format("%04d-%02d-%02d", dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth());
            int logHour = dateTime.getHour();

            // Parse request "GET /path HTTP/1.0"
            String[] reqParts = request.split(" ");
            String httpMethod = reqParts.length > 0 ? reqParts[0] : "";
            String resourcePath = reqParts.length > 1 ? reqParts[1] : "";
            String protocolVersion = reqParts.length > 2 ? reqParts[2] : "";

            long bytesTransferred = 0;
            if (!bytesStr.equals("-")) {
                bytesTransferred = Long.parseLong(bytesStr);
            }

            return new ParsedLog(host, timestampStr, logDate, logHour, httpMethod, resourcePath, protocolVersion, statusCode, bytesTransferred);
        } catch (Exception e) {
            return null;
        }
    }
}
