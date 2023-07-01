package com.mapreduce;

public enum Protocol {
    NODESLIST("NODESLIST"),
    MAPPING("MAPPING"),
    SHUFFLING("SHUFFLING"),
    SORTSHUFFLING("SORTHUFFLING"),
    DONE("DONE"),
    MINMAX("MINMAX"),
    RESULT("RESULT"),
    END("END"),
    QUIT("QUIT");

    private String message;
 
    Protocol(String newMessage) {
        message = newMessage;
    }
 
    public String string() {
        return message;
    }
}
