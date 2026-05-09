package com.invincibleagam.core;

/**
 * Defines how records are grouped into logical batches for processing.
 * 
 * FIXED   - Traditional fixed-size batches (e.g. 10000 lines per batch)
 * MONTHLY - Group records by calendar month (e.g. all Jul-1995 records in one batch)
 * WEEKLY  - Group records by ISO week (e.g. week 27 of 1995 in one batch)
 */
public enum BatchStrategy {
    FIXED("fixed"),
    MONTHLY("monthly"),
    WEEKLY("weekly");

    private final String label;

    BatchStrategy(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    public static BatchStrategy fromString(String s) {
        switch (s.toLowerCase()) {
            case "monthly": case "month": return MONTHLY;
            case "weekly":  case "week":  return WEEKLY;
            default:                       return FIXED;
        }
    }
}
