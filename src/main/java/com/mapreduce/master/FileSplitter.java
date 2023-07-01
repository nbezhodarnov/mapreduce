package com.mapreduce.master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import com.mapreduce.Logger;
import com.mapreduce.Logger.LogLevel;
import com.mapreduce.utils.StringUtils;

public class FileSplitter {
    private static final int ENDOFDOCUMENT = -1;
    private int splitsCount = 1;

    public FileSplitter(int newSplitsCount) {
        splitsCount = newSplitsCount;
    }

    public List<String> splitFile(String fileName) throws Exception {
        ArrayList<String> splits = new ArrayList<>();

        File file = new File(fileName);

        if (!file.exists() && !file.isFile()) {
            Logger.log("Trying to read something, which isn't a file", Logger.LogLevel.Error);
            throw new Exception("Trying to read something, which isn't a file");
        }

        long fileSize = file.length();
        long fileSplitSize = (long)Math.ceil((double)fileSize / (double)splitsCount);
        Logger.log("File split size: " + fileSplitSize, LogLevel.Debug);

        if (fileSplitSize == 0) {
            Logger.log("Trying to split a very short file", Logger.LogLevel.Error);
            throw new Exception("Trying to split a very short file");
        }

        BufferedReader fileReader = new BufferedReader(new FileReader(file));
        char[] buffer = new char[(int)fileSplitSize];

        for (int i = 0; i < splitsCount; i++) {
            int bytesRead = fileReader.read(buffer, 0, (int)fileSplitSize);
            if (bytesRead == ENDOFDOCUMENT || bytesRead <= 0) {
                splits.add("");
                continue;
            }

            // At this moment we reached the end of file
            if (bytesRead < fileSplitSize) {
                Logger.log("Bytes read from file: " + bytesRead, LogLevel.Debug);
                splits.add(new String(buffer, 0, bytesRead));
                continue;
            }

            StringBuilder splitBuilder = new StringBuilder(new String(buffer, 0, bytesRead));

            char[] character = new char[1];
            character[0] = '1';
            while (!StringUtils.isStopCharacter(character[0])) {
                bytesRead = fileReader.read(character, 0, 1);
                if (bytesRead == ENDOFDOCUMENT) {
                    break;
                }
                splitBuilder.append(character, 0, 1);
            }

            splits.add(splitBuilder.toString());
        }
        fileReader.close();

        return splits;
    }
}
