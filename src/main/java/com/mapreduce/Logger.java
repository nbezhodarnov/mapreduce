package com.mapreduce;

public class Logger {
    private static Logger loggerInstance;

    public enum LogLevel {
        Error (0),
        Warning (1),
        Info (2),
        Debug (3);

        private final int levelIndex;
        LogLevel(int levelIndex) {
            this.levelIndex = levelIndex;
        }
    };

    private final static LogLevel LOG_LEVEL = LogLevel.Info;

    private Logger() {}

    public static synchronized Logger getInstance() {
        if (loggerInstance == null) {
            loggerInstance = new Logger();
        }
        return loggerInstance;
    }

    public synchronized void logMessage(String message) {
        System.out.println(message);
    }

    public static void log(String message) {
        log(message, LogLevel.Info);
    }

    public static void log(String message, LogLevel level) {
        if (level.levelIndex > LOG_LEVEL.levelIndex)
        {
            return;
        }
        
        Logger logger = getInstance();
        logger.logMessage(message);
    }
}
