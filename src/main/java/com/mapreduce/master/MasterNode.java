package com.mapreduce.master;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import com.mapreduce.Logger;
import com.mapreduce.Logger.LogLevel;
import com.mapreduce.utils.SocketUtils;
import com.mapreduce.utils.StringUtils;
import com.mapreduce.Protocol;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;

public class MasterNode {
    private long totalTime = 0;
    private long fileSplitTime = 0;
    private long sendSplitsTime = 0;
    private long mappingTime = 0;
    private long shuffleTime = 0;
    private long sortshuffleTime = 0;

    private String textFilePath;

    private class Configuration {
        public ArrayList<Entry<String, Integer>> serverNodes = null;

        public Configuration(ArrayList<Entry<String, Integer>> newServerNodes) {
            serverNodes = newServerNodes;
        }
    }

    private Configuration configuration = null;

    private Configuration parseConfigurationFile(String configurationFilePath) {
        Scanner fileScanner = null;
        ArrayList<Entry<String, Integer>> serverNodes = new ArrayList<>();
        try {
            File file = new File(configurationFilePath);
            fileScanner = new Scanner(file);

            while (fileScanner.hasNextLine()) {
                String[] addressAndPort = fileScanner.nextLine().split(" ");

                String address = addressAndPort[0];
                int port = Integer.parseInt(addressAndPort[1]);

                serverNodes.add(new AbstractMap.SimpleEntry<>(address, port));
            }
        } catch (Exception exception) {
            Logger.log(exception.toString(), LogLevel.Error);
            Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
        } finally {
            if (fileScanner != null) {
                try {
                    fileScanner.close();
                } catch (Exception exception) {
                    Logger.log(exception.toString(), LogLevel.Error);
                    Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
                }
            }
        }

        return new Configuration(serverNodes);
    }

    private class ServerNodeConnection {
        public String serverName = null;
        public Socket socket = null;
        public BufferedReader is = null;
        public BufferedWriter os = null;

        public ServerNodeConnection(String newServerName, Socket newSocket, BufferedReader newIs,
                BufferedWriter newOs) {
            serverName = newServerName;
            socket = newSocket;
            is = newIs;
            os = newOs;
        }
    }

    ArrayList<ServerNodeConnection> serverNodeConnections = null;

    private void connectToServerNodes() {
        for (Entry<String, Integer> serverNode : configuration.serverNodes) {
            boolean unconnected = true;
            while (unconnected) {
                try {
                    Socket serverNodeSocket = new Socket(serverNode.getKey(), serverNode.getValue());
                    BufferedReader serverNodeInputStream = new BufferedReader(
                            new InputStreamReader(serverNodeSocket.getInputStream()));
                    BufferedWriter serverNodeOutputStream = new BufferedWriter(
                            new OutputStreamWriter(serverNodeSocket.getOutputStream()));

                    serverNodeConnections
                            .add(new ServerNodeConnection(serverNode.getKey(), serverNodeSocket, serverNodeInputStream,
                                    serverNodeOutputStream));
                    unconnected = false;
                } catch (Exception exception) {
                    Logger.log(exception.toString(), LogLevel.Error);
                    Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
                }
            }
        }
        ;
    }

    private void sendNodesList() {
        try {
            Logger.log("Sending nodes list to each server node");

            for (ServerNodeConnection serverNodeConnection : serverNodeConnections) {
                try {
                    SocketUtils.sendMessage(serverNodeConnection.os, Protocol.NODESLIST.string());

                    configuration.serverNodes.forEach((Entry<String, Integer> serverNode) -> {
                        if (serverNode.getKey().equals(serverNodeConnection.serverName)) {
                            return;
                        }

                        try {
                            SocketUtils.sendMessage(serverNodeConnection.os, serverNode.getKey() + " " + serverNode.getValue());
                        } catch (IOException exception) {
                            Logger.log(exception.toString(), LogLevel.Error);
                            Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
                        }
                    });

                    SocketUtils.sendMessage(serverNodeConnection.os, Protocol.END.string());
                } catch (IOException exception) {
                    Logger.log(exception.toString(), LogLevel.Error);
                    Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
                }
            }
            ;

            Logger.log("Nodes list has been sent to each server node");
        } catch (Exception exception) {
            Logger.log(exception.toString(), LogLevel.Error);
            Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
        }
    }

    private Thread threadToSendFileContentToServerNode(String split, ServerNodeConnection serverNodeConnection) {
        return new Thread() {
            public void run() {
                try {
                    SocketUtils.sendBytes(serverNodeConnection.os, split);
                } catch (IOException exception) {
                    Logger.log(exception.toString(), LogLevel.Error);
                    Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
                }
            }
        };
    }

    private void mapTextFilesToServerNodes() {
        Scanner fileScanner = null;
        try {
            Logger.log("Launch mapping step");
            for (ServerNodeConnection serverNodeConnection : serverNodeConnections) {
                SocketUtils.sendMessage(serverNodeConnection.os, Protocol.MAPPING.string());
            }

            FileSplitter fileSplitter = new FileSplitter(serverNodeConnections.size());

            long splitFileStart = System.currentTimeMillis();
            List<String> splits = fileSplitter.splitFile(textFilePath);
            long splitFileEnd = System.currentTimeMillis();

            fileSplitTime = splitFileEnd - splitFileStart;
            // Logger.log("File split time: " + fileSplitTime + "ms.", LogLevel.Info);

            long sendSplitsStart = System.currentTimeMillis();
            ArrayList<Thread> sendFilesThreads = new ArrayList<>();
            for (int i = 0; i < serverNodeConnections.size(); i++) {
                sendFilesThreads.add(threadToSendFileContentToServerNode(splits.get(i), serverNodeConnections.get(i)));
            }

            for (Thread sendFileThread : sendFilesThreads) {
                sendFileThread.start();
            }

            for (Thread sendFileThread : sendFilesThreads) {
                sendFileThread.join();
            }
            long sendSplitsEnd = System.currentTimeMillis();

            sendSplitsTime = sendSplitsEnd - sendSplitsStart;
            // Logger.log("Splits send time: " + sendSplitsTime + "ms.", LogLevel.Info);

            for (ServerNodeConnection serverNodeConnection : serverNodeConnections) {
                while (!SocketUtils.waitForMessageAndGet(serverNodeConnection.is).equals(Protocol.DONE.string()))
                    ;
            }

            Logger.log("Mapping step has been finished (master)");
        } catch (Exception exception) {
            Logger.log(exception.toString(), LogLevel.Error);
            Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
        } finally {
            if (fileScanner != null) {
                try {
                    fileScanner.close();
                } catch (Exception exception) {
                    Logger.log(exception.toString(), LogLevel.Error);
                    Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
                }
            }
        }
    }

    private Pair<Integer, Integer> minAndMaxCounts() {
        Integer min = Integer.MAX_VALUE;
        Integer max = 0;

        try {
            if (serverNodeConnections.size() < 1) {
                return Pair.of(0, 0);
            }
            Logger.log("Launch minmax step");

            for (ServerNodeConnection serverNodeConnection : serverNodeConnections) {
                SocketUtils.sendMessage(serverNodeConnection.os, Protocol.MINMAX.string());

                String message = SocketUtils.waitForMessageAndGet(serverNodeConnection.is);

                String[] minAndMaxStrngs = message.split(" ");
                Integer minOnServer = Integer.parseInt(minAndMaxStrngs[0]);
                Integer maxOnServer = Integer.parseInt(minAndMaxStrngs[1]);

                if (minOnServer < min) {
                    min = minOnServer;
                }

                if (maxOnServer > max) {
                    max = maxOnServer;
                }
            }

            Logger.log("Minmax step has been finished");
        } catch (Exception exception) {
            Logger.log(exception.toString(), LogLevel.Error);
            Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
        }

        return Pair.of(min, max);
    }

    private void shuffleServerNodes() {
        try {
            Logger.log("Launch shuffling step");

            for (ServerNodeConnection serverNodeConnection : serverNodeConnections) {
                SocketUtils.sendMessage(serverNodeConnection.os, Protocol.SHUFFLING.string());
            }

            HashMap<Integer, String> distributedRemainders = new HashMap<>();

            int i = 0;
            for (ServerNodeConnection serverNodeConnection : serverNodeConnections) {
                String message = Integer.toString(i);
                SocketUtils.sendMessage(serverNodeConnection.os, message);
                distributedRemainders.put(i, serverNodeConnection.serverName);
                i++;
            }

            for (ServerNodeConnection serverNodeConnection : serverNodeConnections) {
                for (Integer remainder : distributedRemainders.keySet()) {
                    String serverAddress = distributedRemainders.get(remainder);
                    if (serverAddress.equals(serverNodeConnection.serverName)) {
                        continue;
                    }

                    String message = serverAddress + " " + Integer.toString(remainder);
                    SocketUtils.sendMessage(serverNodeConnection.os, message);
                }

                SocketUtils.sendMessage(serverNodeConnection.os, Protocol.END.string());
            }

            for (ServerNodeConnection serverNodeConnection : serverNodeConnections) {
                String message = SocketUtils.waitForMessageAndGet(serverNodeConnection.is);
                if (!message.equals(Protocol.DONE.string())) {
                    Logger.log("Error while shuffling", LogLevel.Error);
                }
            }

            Logger.log("Shuffling step has been finished");
        } catch (Exception exception) {
            Logger.log(exception.toString(), LogLevel.Error);
            Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
        }
    }

    private void sortshuffleServerNodes() {
        try {
            Pair<Integer, Integer> minMax = minAndMaxCounts();
            Integer min = minMax.getLeft();
            Integer max = minMax.getRight();

            Logger.log("Launch sortshuffling step");

            for (ServerNodeConnection serverNodeConnection : serverNodeConnections) {
                SocketUtils.sendMessage(serverNodeConnection.os, Protocol.SORTSHUFFLING.string());
            }

            Integer serversCount = serverNodeConnections.size();
            Integer countsNumberToSortOnServerNode = (max - min + 1) / serversCount;
            Integer restCountsNumberToSort = (max - min + 1) % serversCount;

            Integer minOnServerNode = min;
            Integer maxOnServerNode = minOnServerNode + Integer.max(countsNumberToSortOnServerNode - 1, 0);
            HashMap<String, Pair<Integer, Integer>> distributedCounts = new HashMap<>();
            for (ServerNodeConnection serverNodeConnection : serverNodeConnections) {
                if (restCountsNumberToSort > 0) {
                    if (countsNumberToSortOnServerNode > 0) {
                        maxOnServerNode++;
                    }
                    restCountsNumberToSort--;
                }

                String message = Integer.toString(minOnServerNode) + " " + Integer.toString(maxOnServerNode);
                SocketUtils.sendMessage(serverNodeConnection.os, message);

                distributedCounts.put(serverNodeConnection.serverName, Pair.of(minOnServerNode, maxOnServerNode));

                minOnServerNode = maxOnServerNode + 1;
                maxOnServerNode = minOnServerNode + Integer.max(countsNumberToSortOnServerNode - 1, 0);
            }

            for (ServerNodeConnection serverNodeConnection : serverNodeConnections) {
                for (Map.Entry<String, Pair<Integer, Integer>> serverWithCounts : distributedCounts.entrySet()) {
                    String serverAddress = serverWithCounts.getKey();
                    if (serverAddress.equals(serverNodeConnection.serverName)) {
                        continue;
                    }

                    minOnServerNode = serverWithCounts.getValue().getLeft();
                    maxOnServerNode = serverWithCounts.getValue().getRight();

                    String message = serverAddress + " " + Integer.toString(minOnServerNode) + " "
                            + Integer.toString(maxOnServerNode);
                    SocketUtils.sendMessage(serverNodeConnection.os, message);
                }

                SocketUtils.sendMessage(serverNodeConnection.os, Protocol.END.string());
            }

            for (ServerNodeConnection serverNodeConnection : serverNodeConnections) {
                String message = SocketUtils.waitForMessageAndGet(serverNodeConnection.is);
                if (!message.equals(Protocol.DONE.string())) {
                    Logger.log("Error while shuffling", LogLevel.Error);
                }
            }

            Logger.log("Sortshuffling step has been finished");
        } catch (Exception exception) {
            Logger.log(exception.toString(), LogLevel.Error);
            Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
        }
    }

    private void outputResultFromServerNode() {
        try {
            if (serverNodeConnections.size() < 1) {
                Logger.log("Requested result without any server");
                System.out.println("Nothing to output");
                return;
            }

            Logger.log("Launch result step");

            for (ServerNodeConnection serverNodeConnection : serverNodeConnections) {
                SocketUtils.sendMessage(serverNodeConnection.os, Protocol.RESULT.string());

                StringUtils.computeWithWordsFromMessage(SocketUtils.readBytes(serverNodeConnection.is), (word, count) -> {
                    System.out.println(word + ": " + count);
                });
            }

            Logger.log("Result step has been finished");
        } catch (Exception exception) {
            Logger.log(exception.toString(), LogLevel.Error);
            Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
        }
    }

    protected void finalize() {
        for (ServerNodeConnection serverNodeConnection : serverNodeConnections) {
            try {
                SocketUtils.sendMessage(serverNodeConnection.os, Protocol.QUIT.string());

                if (!SocketUtils.waitForMessageAndGet(serverNodeConnection.is).equals(Protocol.DONE.string())) {
                    Logger.log("Server didn't stop correctly!", LogLevel.Error);
                }

                serverNodeConnection.is.close();
                serverNodeConnection.os.close();
                serverNodeConnection.socket.close();
            } catch (IOException exception) {
                Logger.log(exception.toString(), LogLevel.Error);
                Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
            }
        }

        Logger.log("Master node has been stopped!");
    }

    public MasterNode(String configurationFilePath, String newTextFilePath) throws InterruptedException {
        textFilePath = newTextFilePath;
        configuration = parseConfigurationFile(configurationFilePath);
        serverNodeConnections = new ArrayList<>();

        Logger.log("Master node has been launched");

        connectToServerNodes();
        sendNodesList();

        long totalStart = System.currentTimeMillis();

        long mappingStart = System.currentTimeMillis();
        mapTextFilesToServerNodes();
        long mappingEnd = System.currentTimeMillis();

        mappingTime = mappingEnd - mappingStart;
        //Logger.log("Mapping time: " + mappingTime + "ms.", LogLevel.Info);

        long shuffleStart = System.currentTimeMillis();
        shuffleServerNodes();
        long shuffleEnd = System.currentTimeMillis();

        shuffleTime = shuffleEnd - shuffleStart;
        //Logger.log("Shuffle time: " + shuffleTime + "ms.", LogLevel.Info);

        long sortshuffleStart = System.currentTimeMillis();
        sortshuffleServerNodes();
        long sortshuffleEnd = System.currentTimeMillis();

        sortshuffleTime = sortshuffleEnd - sortshuffleStart;

        long totalEnd = System.currentTimeMillis();

        totalTime = totalEnd - totalStart;

        outputResultFromServerNode();

        Logger.log("File split time: " + fileSplitTime + "ms.", LogLevel.Info);
        Logger.log("Splits send time: " + sendSplitsTime + "ms.", LogLevel.Info);
        Logger.log("Mapping time: " + mappingTime + "ms.", LogLevel.Info);
        Logger.log("Shuffle time: " + shuffleTime + "ms.", LogLevel.Info);
        Logger.log("Sortshuffle time: " + sortshuffleTime + "ms.", LogLevel.Info);
        Logger.log("Total time: " + totalTime + "ms.", LogLevel.Info);
    }

    static public void main(String[] args) throws InterruptedException {
        if (args.length < 1) {
            System.out.println("Provide a file!");
            return;
        }

        MasterNode masterNode = new MasterNode("configuration.txt", args[0]);
        Thread.sleep(5000);
        masterNode.finalize();
    }
}
