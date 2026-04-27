"""
Shared NASA HTTP Log Parser.

Parses lines from the NASA Kennedy Space Center HTTP access logs (July/August 1995).
This module is the SINGLE SOURCE OF TRUTH for parsing rules — every pipeline
must use equivalent logic.

Sample log line:
  unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985
"""

import re
from datetime import datetime

# ─── Regex pattern ────────────────────────────────────────────────────────────
# Group 1: host
# Group 2: full timestamp  (01/Jul/1995:00:00:06 -0400)
# Group 3: request string  (GET /shuttle/countdown/ HTTP/1.0)
# Group 4: status code     (200)
# Group 5: bytes           (3985 or -)
LOG_PATTERN = re.compile(
    r'^(\S+)'                           # host
    r'\s+\S+\s+\S+'                     # ident + authuser (always "- -")
    r'\s+\[([^\]]+)\]'                  # [timestamp]
    r'\s+"([^"]*)"'                     # "request"
    r'\s+(\d{3})'                       # status code
    r'\s+(\S+)$'                        # bytes (or -)
)

# Sub-pattern for the request string: METHOD /path PROTOCOL
REQUEST_PATTERN = re.compile(
    r'^(\S+)\s+(\S+)(?:\s+(\S+))?$'    # method  path  [protocol]
)

# Timestamp format in the logs
TIMESTAMP_FORMAT = "%d/%b/%Y:%H:%M:%S %z"

# Month abbreviation mapping for fast date parsing (avoids strptime overhead)
MONTH_MAP = {
    'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
    'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
    'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12',
}


def parse_log_line(line):
    """
    Parse a single NASA HTTP log line into a structured dictionary.

    Returns:
        dict with keys: host, timestamp, log_date, log_hour, http_method,
                        resource_path, protocol_version, status_code, bytes_transferred
        None if the line is malformed (caller must track malformed count).
    """
    line = line.strip()
    if not line:
        return None

    match = LOG_PATTERN.match(line)
    if not match:
        return None

    host = match.group(1)
    timestamp_str = match.group(2)
    request_str = match.group(3)
    status_code = int(match.group(4))
    bytes_str = match.group(5)

    # ── Parse bytes (- → 0) ──────────────────────────────────────────────
    bytes_transferred = 0 if bytes_str == '-' else int(bytes_str)

    # ── Parse request string ─────────────────────────────────────────────
    req_match = REQUEST_PATTERN.match(request_str)
    if req_match:
        http_method = req_match.group(1)
        resource_path = req_match.group(2)
        protocol_version = req_match.group(3) if req_match.group(3) else ""
    else:
        # Some requests are malformed but the outer regex matched.
        # Treat the entire request as the resource path.
        http_method = ""
        resource_path = request_str
        protocol_version = ""

    # ── Parse timestamp ──────────────────────────────────────────────────
    # Fast manual parsing:  "01/Jul/1995:00:00:06 -0400"
    try:
        day = timestamp_str[0:2]
        month_abbr = timestamp_str[3:6]
        year = timestamp_str[7:11]
        hour = timestamp_str[12:14]
        month_num = MONTH_MAP.get(month_abbr, '00')
        log_date = f"{year}-{month_num}-{day}"   # ISO format: 1995-07-01
        log_hour = int(hour)
    except (IndexError, ValueError):
        return None

    return {
        "host":              host,
        "timestamp":         timestamp_str,
        "log_date":          log_date,
        "log_hour":          log_hour,
        "http_method":       http_method,
        "resource_path":     resource_path,
        "protocol_version":  protocol_version,
        "status_code":       status_code,
        "bytes_transferred": bytes_transferred,
    }


def parse_log_line_tuple(line):
    """
    Same as parse_log_line but returns a tuple for MapReduce text output.
    Order: (host, timestamp, log_date, log_hour, http_method, resource_path,
            protocol_version, status_code, bytes_transferred)
    Returns None for malformed lines.
    """
    result = parse_log_line(line)
    if result is None:
        return None
    return (
        result["host"],
        result["timestamp"],
        result["log_date"],
        result["log_hour"],
        result["http_method"],
        result["resource_path"],
        result["protocol_version"],
        result["status_code"],
        result["bytes_transferred"],
    )
