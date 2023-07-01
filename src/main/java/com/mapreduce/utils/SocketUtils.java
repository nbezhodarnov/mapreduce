package com.mapreduce.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;

import com.mapreduce.Logger;
import com.mapreduce.Logger.LogLevel;

public class SocketUtils {
    public static void sendBytes(BufferedWriter writer, String message) throws IOException {
        sendMessage(writer, Integer.toString(message.length()));
        writer.write(message);
        writer.flush();
    }

    public static void sendMessage(BufferedWriter writer, String message) throws IOException {
        Logger.log("Message to send: " + message, LogLevel.Debug);
        writer.write(message);
        writer.newLine();
        writer.flush();
    }

    public static String readBytes(BufferedReader reader) throws IOException {
        String message = waitForMessageAndGet(reader);
        Integer bytesToReceive = Integer.parseInt(message);
        return readBytes(reader, bytesToReceive);
    }

    public static String readBytes(Reader reader, int bytesNumber) throws IOException {
        int bytesToReceive = bytesNumber;
        char[] buffer = new char[bytesToReceive];

        StringBuilder messageBuilder = new StringBuilder();
        while (bytesToReceive > 0) {
            int receivedBytes = reader.read(buffer, 0, bytesToReceive);

            if (receivedBytes <= 0) {
                continue;
            }

            bytesToReceive -= receivedBytes;
            messageBuilder.append(buffer, 0, receivedBytes);
        }

        return messageBuilder.toString();
    }

    public static String waitForMessageAndGet(BufferedReader reader) throws IOException {
        String message = null;
        while ((message = reader.readLine()) == null || message.isEmpty())
            ;
        Logger.log("Message received: " + message, LogLevel.Debug);
        return message;
    }
}
