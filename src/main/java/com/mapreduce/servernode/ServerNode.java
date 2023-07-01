package com.mapreduce.servernode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.mapreduce.Logger;
import com.mapreduce.Protocol;
import com.mapreduce.Logger.LogLevel;
import com.mapreduce.utils.SocketUtils;

public class ServerNode {
    private AtomicBoolean readyToShuffle = new AtomicBoolean(false);
    private AtomicBoolean inShuffleMode = new AtomicBoolean(false);
    private AtomicInteger shufflingThreads = new AtomicInteger(0);

    private WordsCounter wordsCounter = null;

    private ServerSocket listener = null;
    private Socket socketOfServer = null;
    private BufferedReader is = null;
    private BufferedWriter os = null;

    private class ServerNodeConnection {
        public Socket socket = null;
        public BufferedWriter os = null;

        public ServerNodeConnection(Socket newSocket) throws IOException {
            socket = newSocket;
            os = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        }
    }

    private HashMap<String, ServerNodeConnection> otherServerNodes = null;
    private ArrayList<Thread> otherServerNodesThreads = null;

    public ServerNode(int port) {
        wordsCounter = new WordsCounter();

        otherServerNodes = new HashMap<>();
        otherServerNodesThreads = new ArrayList<Thread>();
        launchServer(port);
    }

    public class MessagesFromOtherServerNodeHandler implements Runnable {
        public MessagesFromOtherServerNodeHandler() {
        }

        public void run() {
            Socket otherServerSocket = null;
            String remoteAddress = null;
            BufferedReader otherServerIS = null;

            try {
                Logger.log("Accepting other server node");
                otherServerSocket = listener.accept();
                Logger.log("Accepted other server node");

                remoteAddress = otherServerSocket.getRemoteSocketAddress().toString();

                otherServerIS = new BufferedReader(
                        new InputStreamReader(otherServerSocket.getInputStream()));
            } catch (IOException exception) {
                Logger.log(
                        "Connection to server node with address "
                                + remoteAddress
                                + " has been refused",
                        LogLevel.Error);
                Logger.log(exception.toString(), LogLevel.Error);
                Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
            }

            try {
                while (true) {
                    try {
                        while (!readyToShuffle.get())
                            ;
                        // Receive number of lines that will be sent
                        String message = SocketUtils.waitForMessageAndGet(otherServerIS);
                        Logger.log("Receiving data from other server node by address " + remoteAddress);

                        inShuffleMode.set(true);
                        shufflingThreads.incrementAndGet();

                        Integer bytesToReceive = Integer.parseInt(message);
                        message = SocketUtils.readBytes(otherServerIS, bytesToReceive);

                        Logger.log("Successfully received data from other server node by address " + remoteAddress);

                        Logger.log("Merging data from other server node by address " + remoteAddress);

                        wordsCounter.countWordsFromStringMessage(message);

                        shufflingThreads.decrementAndGet();

                        Logger.log("Successfully merged data from other server node by address " + remoteAddress);
                    } catch (Exception exception) {
                        Logger.log(
                                "Unknown error in connection to server node with address "
                                        + remoteAddress,
                                LogLevel.Error);
                        Logger.log(exception.toString(), LogLevel.Error);
                        Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
                        break;
                    }
                }
            } catch (Exception exception) {
                Logger.log(
                        "Unknown error in connection to server node with address "
                                + remoteAddress,
                        LogLevel.Error);
                Logger.log(exception.toString(), LogLevel.Error);
                Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
            } finally {
                try {
                    if (otherServerIS != null) {
                        otherServerIS.close();
                    }

                    if (otherServerSocket != null) {
                        otherServerSocket.close();
                    }
                } catch (IOException exception) {
                    Logger.log(
                            "Error while closing a connection to server node with address "
                                    + remoteAddress,
                            LogLevel.Error);
                    Logger.log(exception.toString(), LogLevel.Error);
                    Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
                }
            }
        }
    }

    private synchronized void connectToServerNode(String serverNodeAddress, int port) {
        try {
            Socket otherServerNodeSocket = new Socket(serverNodeAddress, port);

            otherServerNodes.put(serverNodeAddress, new ServerNodeConnection(otherServerNodeSocket));
        } catch (IOException exception) {
            Logger.log("Failed to connect to node " + serverNodeAddress + " by port " + port, LogLevel.Error);
            Logger.log(exception.toString(), LogLevel.Error);
            Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
        }
    }

    private void receiveServerNodesListAndConnect() throws IOException {
        Logger.log("Receiving server nodes list");
        disconnectFromServerNodes();

        ArrayList<Map.Entry<String, Integer>> otherServerNodesList = new ArrayList<>();
        String line = SocketUtils.waitForMessageAndGet(is);
        while (!line.equals(Protocol.END.string())) {
            String[] addressAndPort = line.split(" ");
            String address = addressAndPort[0];
            int otherServerNodePort = Integer.parseInt(addressAndPort[1]);
            otherServerNodesList.add(new AbstractMap.SimpleEntry<>(address, otherServerNodePort));

            Logger.log("Connected to server node by address " + address + " and port " + otherServerNodePort);

            line = SocketUtils.waitForMessageAndGet(is);
        }

        for (int i = 0; i < otherServerNodesList.size(); i++) {
            otherServerNodesThreads
                    .add(new Thread(
                            new MessagesFromOtherServerNodeHandler()));
            otherServerNodesThreads.get(otherServerNodesThreads.size() - 1).start();
        }

        for (Map.Entry<String, Integer> otherServerNode : otherServerNodesList) {
            connectToServerNode(otherServerNode.getKey(), otherServerNode.getValue());
        }
        Logger.log("Received server nodes list");
    }

    private void mapping() throws IOException {
        Logger.log("Mapping step has been started");
        String data = SocketUtils.readBytes(is);
        wordsCounter.countWordsFromString(data);

        SocketUtils.sendMessage(os, Protocol.DONE.string());

        Logger.log("Mapping step has been finished");
    }

    private void shuffling() throws IOException {
        Logger.log("Shuffling step has been started");

        String line = SocketUtils.waitForMessageAndGet(is);
        Integer remainder = Integer.parseInt(line);

        HashMap<Integer, String> otherServerNodesRemainders = new HashMap<>();
        line = SocketUtils.waitForMessageAndGet(is);
        while (!line.equals(Protocol.END.string())) {
            String[] addressAndRemainder = line.split(" ");
            String address = addressAndRemainder[0];
            int otherServerNodeRemainder = Integer.parseInt(addressAndRemainder[1]);
            otherServerNodesRemainders.put(otherServerNodeRemainder, address);

            line = SocketUtils.waitForMessageAndGet(is);
        }

        HashMap<String, StringBuilder> addressToMessageMapper = new HashMap<>();
        WordsCounter result = new WordsCounter();

        int serverNodesNumber = otherServerNodes.size() + 1;

        for (Map.Entry<String, Integer> wordWithCount : wordsCounter.getWordsCounts()) {
            String word = wordWithCount.getKey();
            Integer count = wordWithCount.getValue();

            int currentRemainder = word.hashCode() % serverNodesNumber;
            if (currentRemainder < 0) {
                currentRemainder += serverNodesNumber;
            }

            if (currentRemainder == remainder) {
                result.add(word, count);
            } else {
                String message = Integer.toString(count) + " " +
                        word.replaceAll("[\n|\r]", "")
                        + "\n";

                String otherServerNodeAddress = otherServerNodesRemainders.get(currentRemainder);

                addressToMessageMapper.compute(otherServerNodeAddress,
                    (k, v) -> (v == null) ? new StringBuilder(message)
                            : v.append(message));
            }
        }

        wordsCounter = result;

        readyToShuffle.set(true);

        for (String otherServerNodeAddress : otherServerNodes.keySet()) {
            if (!addressToMessageMapper.containsKey(otherServerNodeAddress)) {
                addressToMessageMapper.put(otherServerNodeAddress, new StringBuilder());
            }
        }

        for (String receiver : addressToMessageMapper.keySet()) {
            Logger.log("Sending shuffle data to " + receiver, LogLevel.Debug);
            ServerNodeConnection otherServerNode = otherServerNodes.get(receiver);

            String messageToSend = addressToMessageMapper.get(receiver).toString();

            try {
                SocketUtils.sendBytes(otherServerNode.os, messageToSend);
            } catch (IOException exception) {
                Logger.log(exception.toString(), LogLevel.Error);
                Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
            }
        }

        while (!inShuffleMode.get())
            ;

        while (shufflingThreads.get() > 0)
            ;

        inShuffleMode.set(false);
        readyToShuffle.set(false);

        SocketUtils.sendMessage(os, Protocol.DONE.string());

        Logger.log("Shuffling step has been finished");
    }

    private void sortshuffling() throws IOException {
        Logger.log("Sortshuffling step has been started");

        String line = SocketUtils.waitForMessageAndGet(is);
        String[] minAndMaxString = line.split(" ");
        Integer min = Integer.parseInt(minAndMaxString[0]);
        Integer max = Integer.parseInt(minAndMaxString[1]);

        ArrayList<Pair<String, Pair<Integer, Integer>>> otherServerNodesMinMax = new ArrayList<>();
        line = SocketUtils.waitForMessageAndGet(is);
        while (!line.equals(Protocol.END.string())) {
            String[] addressAndMinMax = line.split(" ");
            String address = addressAndMinMax[0];
            int otherServerNodeMin = Integer.parseInt(addressAndMinMax[1]);
            int otherServerNodeMax = Integer.parseInt(addressAndMinMax[2]);
            otherServerNodesMinMax.add(Pair.of(address, Pair.of(otherServerNodeMin, otherServerNodeMax)));

            line = SocketUtils.waitForMessageAndGet(is);
        }

        Map<Integer, List<String>> wordsDistributedByCount = wordsCounter.getWordsDistributedByCount();

        HashMap<String, StringBuilder> addressToMessageMapper = new HashMap<>();
        WordsCounter result = new WordsCounter();

        for (Integer count : wordsDistributedByCount.keySet()) {
            if (min <= count && count <= max) {
                result.merge(
                        new AbstractMap.SimpleEntry<Integer, List<String>>(count, wordsDistributedByCount.get(count)));
            } else {
                String message = Integer.toString(count) + " " +
                        String.join(" ", wordsDistributedByCount.get(count))
                                .replaceAll("[\n|\r]", "")
                        + "\n";

                for (Pair<String, Pair<Integer, Integer>> otherServerNodeMinMax : otherServerNodesMinMax) {
                    Integer otherServerNodeMin = otherServerNodeMinMax.getRight().getLeft();
                    Integer otherServerNodeMax = otherServerNodeMinMax.getRight().getRight();

                    if (otherServerNodeMin <= count && count <= otherServerNodeMax) {
                        String otherServerNodeAddress = otherServerNodeMinMax.getLeft();
                        addressToMessageMapper.compute(otherServerNodeAddress,
                                (k, v) -> (v == null) ? new StringBuilder(message)
                                        : v.append(message));
                        break;
                    }
                }
            }
        }

        wordsCounter = result;

        readyToShuffle.set(true);

        for (String otherServerNodeAddress : otherServerNodes.keySet()) {
            if (!addressToMessageMapper.containsKey(otherServerNodeAddress)) {
                addressToMessageMapper.put(otherServerNodeAddress, new StringBuilder());
            }
        }

        for (String receiver : addressToMessageMapper.keySet()) {
            ServerNodeConnection otherServerNode = otherServerNodes.get(receiver);

            String messageToSend = addressToMessageMapper.get(receiver).toString();

            try {
                SocketUtils.sendBytes(otherServerNode.os, messageToSend);
            } catch (IOException exception) {
                Logger.log(exception.toString(), LogLevel.Error);
                Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
            }
        }

        while (!inShuffleMode.get())
            ;

        while (shufflingThreads.get() > 0)
            ;

        inShuffleMode.set(false);
        readyToShuffle.set(false);

        SocketUtils.sendMessage(os, Protocol.DONE.string());

        Logger.log("Sortshuffling step has been finished");
    }

    private void sendMinMaxCounts() throws IOException {
        Logger.log("Sending minmax to master");

        Pair<Integer, Integer> minMax = wordsCounter.minAndMaxCounts();

        String message = Integer.toString(minMax.getLeft()) + " " + Integer.toString(minMax.getRight());
        SocketUtils.sendMessage(os, message);

        Logger.log("Sent minmax to master");
    }

    private void sendResultToMaster() throws IOException {
        Logger.log("Sending result to master");

        List<Map.Entry<Integer, List<String>>> wordsCounts = new ArrayList<>(
                wordsCounter.getWordsDistributedByCount().entrySet());

        Collections.sort(wordsCounts, new Comparator<Map.Entry<Integer, List<String>>>() {
            public int compare(
                    Map.Entry<Integer, List<String>> object1,
                    Map.Entry<Integer, List<String>> object2) {
                return (object1.getKey()).compareTo(object2.getKey());
            }
        });

        StringBuilder messageBuilder = new StringBuilder();
        for (Map.Entry<Integer, List<String>> wordsByCount : wordsCounts) {
            List<String> words = wordsByCount.getValue();
            Collections.sort(words);

            messageBuilder.append(
                    Integer.toString(wordsByCount.getKey()) + " " + String.join(" ", words)
                            .replaceAll("[\n|\r]", "")
                            + "\n");
        }
        SocketUtils.sendBytes(os, messageBuilder.toString());

        Logger.log("Sent result to master");
    }

    private void launchServer(int port) {
        // Try to open a server socket on port 10325
        // Note that we can't choose a port less than 1023 if we are not
        // privileged users (root)

        try {
            listener = new ServerSocket(port);
        } catch (IOException exception) {
            Logger.log(exception.toString(), LogLevel.Error);
            Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
            System.exit(1);
        }

        try {
            Logger.log("Server is waiting to accept master node...");

            // Accept client connection request
            // Get new Socket at Server.
            socketOfServer = listener.accept();
            Logger.log("Accepted the master node");

            // Open input and output streams
            is = new BufferedReader(new InputStreamReader(socketOfServer.getInputStream()));
            os = new BufferedWriter(new OutputStreamWriter(socketOfServer.getOutputStream()));
        } catch (Exception exception) {
            Logger.log(exception.toString(), LogLevel.Error);
            Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);

            return;
        }

        while (true) {
            try {
                String line = SocketUtils.waitForMessageAndGet(is);

                if (line.equals(Protocol.NODESLIST.string())) {
                    receiveServerNodesListAndConnect();
                } else if (line.equals(Protocol.MAPPING.string())) {
                    mapping();
                } else if (line.equals(Protocol.SHUFFLING.string())) {
                    shuffling();
                } else if (line.equals(Protocol.SORTSHUFFLING.string())) {
                    sortshuffling();
                } else if (line.equals(Protocol.MINMAX.string())) {
                    sendMinMaxCounts();
                } else if (line.equals(Protocol.RESULT.string())) {
                    sendResultToMaster();
                } else if (line.equals(Protocol.QUIT.string())) {
                    Logger.log("Shutting down server by command");

                    SocketUtils.sendMessage(os, Protocol.DONE.string());
                    break;
                }
            } catch (Exception exception) {
                Logger.log(exception.toString(), LogLevel.Error);
                Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
            }
        }
    }

    private void disconnectFromServerNodes() throws IOException {
        for (Thread therad : otherServerNodesThreads) {
            therad.interrupt();
        }

        for (ServerNodeConnection otherServerNode : otherServerNodes.values()) {
            otherServerNode.os.close();
            otherServerNode.socket.close();
        }
    }

    protected void finalize() throws IOException {
        is.close();
        os.close();

        disconnectFromServerNodes();

        socketOfServer.close();
        listener.close();

        Logger.log("Server has been stopped");
    }

    static public void main(String[] args) {
        try {
            if (args.length < 1) {
                System.out.println("Provide a port!");
                return;
            }

            int port = Integer.parseInt(args[0]);
            ServerNode serverNode = new ServerNode(port);
            serverNode.finalize();
        } catch (Exception exception) {
            Logger.log(exception.toString(), LogLevel.Error);
            Logger.log(ExceptionUtils.getStackTrace(exception), LogLevel.Error);
        }
    }
}
