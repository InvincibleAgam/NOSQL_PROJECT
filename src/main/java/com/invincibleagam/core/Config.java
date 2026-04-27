package com.invincibleagam.core;

public class Config {
    // Data settings
    public static final String DATA_DIR = "data/raw";
    public static final String JULY_LOG = DATA_DIR + "/NASA_access_log_Jul95";
    public static final String AUGUST_LOG = DATA_DIR + "/NASA_access_log_Aug95";
    public static final int DEFAULT_BATCH_SIZE = 10000;

    // Database configurations
    public static final String MYSQL_HOST = "localhost";
    public static final int MYSQL_PORT = 3307;
    public static final String MYSQL_USER = "root";
    public static final String MYSQL_PASSWORD = "";
    public static final String MYSQL_DATABASE = "nasa_log_analysis";

    public static final String MONGO_HOST = "localhost";
    public static final int MONGO_PORT = 27017;
    public static final String MONGO_DATABASE = "nasa_log_analysis";
    public static final String MONGO_COLLECTION = "logs";
}
