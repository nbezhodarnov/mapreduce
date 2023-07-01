package com.mapreduce.servernode;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.mapreduce.Logger;
import com.mapreduce.Logger.LogLevel;
import com.mapreduce.utils.StringUtils;

import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;

public class WordsCounter {
    private HashMap<String, Integer> wordsCounter;
    private Comparator<Map.Entry<String, Integer>> comparator;

    public WordsCounter() {
        wordsCounter = new HashMap<>();
        comparator = new Comparator<Map.Entry<String, Integer>>() {
            public int compare(
                    Map.Entry<String, Integer> object1,
                    Map.Entry<String, Integer> object2) {
                if ((object1.getValue()).compareTo(object2.getValue()) == 0) {
                    return (object1.getKey()).compareTo(object2.getKey());
                }

                return -(object1.getValue()).compareTo(object2.getValue());
            }
        };
    }

    public synchronized void countWordsFromString(String string) {
        if (string == null) {
            return;
        }

        StringBuilder wordBuilder = new StringBuilder();
        for (int i = 0; i < string.length(); i++) {
            if (!StringUtils.isStopCharacter(string.charAt(i))) {
                wordBuilder.append(string.charAt(i));
                continue;
            }

            if (string.charAt(i) == '\r') {
                continue;
            }

            if (wordBuilder.length() <= 0) {
                continue;
            }

            wordsCounter.compute(wordBuilder.toString().trim(), (k, v) -> (v == null) ? 1 : v + 1);
            wordBuilder.setLength(0);
        }

        if (wordBuilder.length() > 0) {
            wordsCounter.compute(wordBuilder.toString().trim(), (k, v) -> (v == null) ? 1 : v + 1);
            wordBuilder.setLength(0);
        }
    }

    public synchronized void countWordsFromStringMessage(String message) {
        StringUtils.computeWithWordsFromMessage(message, (word, count) -> {
            wordsCounter.compute(word, (k, v) -> (v == null) ? count : v + count);
        });
    }

    public Pair<Integer, Integer> minAndMaxCounts() {
        Integer min = Integer.MAX_VALUE;
        Integer max = 0;
        for (Integer count : wordsCounter.values()) {
            if (count < min) {
                min = count;
            }

            if (count > max) {
                max = count;
            }
        }

        return Pair.of(min, max);
    }

    public Map<Integer, List<String>> getWordsDistributedByCount() {
        HashMap<Integer, List<String>> wordsByCount = new HashMap<>();

        for (Map.Entry<String, Integer> wordWithCount : wordsCounter.entrySet()) {
            Integer count = wordWithCount.getValue();
            List<String> words = wordsByCount.getOrDefault(count, new LinkedList<>());
            words.add(wordWithCount.getKey());
            wordsByCount.put(count, words);
        }

        return wordsByCount;
    }

    public List<Map.Entry<String, Integer>> getWordsWithCountsInRange(int min, int max) {
        LinkedList<Map.Entry<String, Integer>> wordsWithCounts = new LinkedList<>();

        for (Map.Entry<String, Integer> wordWithCount : wordsCounter.entrySet()) {
            Integer count = wordWithCount.getValue();

            if (min <= count && count <= max) {
                wordsWithCounts.add(wordWithCount);
            }
        }

        return wordsWithCounts;
    }

    public List<Map.Entry<String, Integer>> getWordsCounts() {
        return new LinkedList<>(wordsCounter.entrySet());
    }

    public List<Map.Entry<String, Integer>> getSortedWordsCounts() {
        List<Map.Entry<String, Integer>> sortedList = new ArrayList<>(wordsCounter.entrySet());
        sortedList.sort(comparator);
        return sortedList;
    }

    public synchronized void add(String word, Integer count) {
        wordsCounter.compute(word, (k, v) -> (v == null) ? count : v + count);
    }

    public synchronized void merge(Map<String, Integer> wordsCounterFromOutside) {
        for (Map.Entry<String, Integer> wordWithCount : wordsCounterFromOutside.entrySet()) {
            if (wordWithCount.getKey() == null || wordWithCount.getValue() == null) {
                Logger.log("Got some item with null key or null value", LogLevel.Warning);
                continue;
            }
            Integer updatedWordCount = wordsCounter.getOrDefault(wordWithCount.getKey(), 0) + wordWithCount.getValue();
            wordsCounter.put(wordWithCount.getKey(), updatedWordCount);
        }
    }

    public synchronized void merge(Map.Entry<Integer, List<String>> wordsByCount) {
        Integer count = wordsByCount.getKey();
        if (count == null) {
            Logger.log("Got null count", LogLevel.Warning);
            return;
        }

        for (String word : wordsByCount.getValue()) {
            if (word == null) {
                Logger.log("Got null word", LogLevel.Warning);
                continue;
            }

            Integer updatedWordCount = wordsCounter.getOrDefault(word, 0) + count;
            wordsCounter.put(word, updatedWordCount);
        }
    }

    public synchronized void clear() {
        wordsCounter.clear();
    }
}
