package com.mapreduce.utils;

import java.util.function.BiConsumer;

public class StringUtils {
    private static final int UNREADCOUNT = -1;

    static public boolean isStopCharacter(char character) {
        return character == ' ' || character == '\n';
    }

    static public void computeWithWordsFromMessage(String message, BiConsumer<String, Integer> compute) {
        if (message == null) {
            return;
        }

        StringBuilder wordBuilder = new StringBuilder();
        int count = UNREADCOUNT;
        for (int i = 0; i < message.length(); i++) {
            if (!StringUtils.isStopCharacter(message.charAt(i))) {
                if (message.charAt(i) == '\r') {
                    continue;
                }

                wordBuilder.append(message.charAt(i));
                continue;
            }

            if (wordBuilder.length() <= 0) {
                continue;
            }

            if (count == UNREADCOUNT) {
                count = Integer.parseInt(wordBuilder.toString());
                wordBuilder.setLength(0);
                continue;
            }

            compute.accept(wordBuilder.toString().trim(), count);
            wordBuilder.setLength(0);

            if (message.charAt(i) == '\n') {
                count = UNREADCOUNT;
            }
        }
    }
}
