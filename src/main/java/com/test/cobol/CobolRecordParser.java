package com.test.cobol;

import java.nio.file.*;
import java.io.IOException;
import java.util.*;

public class CobolRecordParser {

    static class Record {
        String keyField1;
        int fld3Num;
        int fld1Num;
        int fld2Num;
        List<String> fld1Vec = new ArrayList<>();
        List<String> fld2Vec = new ArrayList<>();
        String fld3Text;

        @Override
        public String toString() {
            return "Record{" +
                    "keyField1='" + keyField1 + '\'' +
                    ", fld3Num=" + fld3Num +
                    ", fld1Num=" + fld1Num +
                    ", fld2Num=" + fld2Num +
                    ", fld1Vec=" + fld1Vec +
                    ", fld2Vec=" + fld2Vec +
                    ", fld3Text='" + fld3Text + '\'' +
                    '}';
        }
    }

    public static Record parseRecord(String line, Map<String, Integer> fieldPositions) {
        Record record = new Record();

        // Parse keyField1
        record.keyField1 = line.substring(fieldPositions.get("KEY-FIELD1-START"), fieldPositions.get("KEY-FIELD1-END")).trim();

        // Parse fld3Num
        record.fld3Num = Integer.parseInt(line.substring(fieldPositions.get("FLD3-NUM-START"), fieldPositions.get("FLD3-NUM-END")));

        // Parse fld1Num
        record.fld1Num = Integer.parseInt(line.substring(fieldPositions.get("FLD1-NUM-START"), fieldPositions.get("FLD1-NUM-END")));

        // Parse fld2Num
        record.fld2Num = Integer.parseInt(line.substring(fieldPositions.get("FLD2-NUM-START"), fieldPositions.get("FLD2-NUM-END")));

        // Parse fld1Vec
        int fld1Start = fieldPositions.get("FLD1-VEC-START");
        int fld1End = fld1Start + (24 * record.fld1Num);
        for (int i = 0; i < record.fld1Num; i++) {
            String fld1Item = line.substring(fld1Start + (i * 24), fld1Start + (i * 24) + 24).trim();
            record.fld1Vec.add(fld1Item);
        }

        // Parse fld2Vec
        int fld2Start = fld1End;
        int fld2End = fld2Start + (33 * record.fld2Num);
        for (int i = 0; i < record.fld2Num; i++) {
            String fld2Item = line.substring(fld2Start + (i * 33), fld2Start + (i * 33) + 33).trim();
            record.fld2Vec.add(fld2Item);
        }

        // Parse fld3Length and fld3Text
        if (record.fld3Num > 0) {
            int fld3LengthStart = fld2End;
            int fld3Length = Integer.parseInt(line.substring(fld3LengthStart, fld3LengthStart + 4));

            int fld3TextStart = fld3LengthStart + 4;
            record.fld3Text = line.substring(fld3TextStart, fld3TextStart + fld3Length).trim();
        } else {
            record.fld3Text = "";
        }

        return record;
    }

    public static Map<String, Integer> parseCopybook(String copybookPath) throws IOException {
        Map<String, Integer> fieldPositions = new HashMap<>();
        List<String> lines = Files.readAllLines(Paths.get(copybookPath));

        int currentPosition = 0;
        for (String line : lines) {
            line = line.trim();
            if (line.startsWith("05")) {
                String[] parts = line.split("\\s+");
                String fieldName = parts[1];
                String fieldType = parts[2];

                int fieldLength = 0;
                if (fieldType.startsWith("PIC X(")) {
                    fieldLength = Integer.parseInt(fieldType.substring(6, fieldType.length() - 1));
                } else if (fieldType.startsWith("PIC 9(")) {
                    fieldLength = Integer.parseInt(fieldType.substring(6, fieldType.length() - 1));
                } else if (fieldType.equals("COMP")) {
                    fieldLength = 4; // Assuming COMP fields are 4 bytes
                }

                fieldPositions.put(fieldName + "-START", currentPosition);
                currentPosition += fieldLength;
                fieldPositions.put(fieldName + "-END", currentPosition);
            }
        }

        return fieldPositions;
    }
    public static Record parseRecord(String line) {
        Record record = new Record();
        System.out.println("Parsing line :"+line);
        // Hardcoded positions and lengths based on copybook structure
        int keyField1Start = 0;
        int keyField1Length = 12;
        int fld3NumStart = keyField1Start + keyField1Length;
        int fld3NumLength = 1;
        int fld1NumStart = fld3NumStart + fld3NumLength;
        int fld1NumLength = 3;
        int fld2NumStart = fld1NumStart + fld1NumLength;
        int fld2NumLength = 3;
        int fld1VecStart = fld2NumStart + fld2NumLength;

        // Parse keyField1
        record.keyField1 = line.substring(keyField1Start, keyField1Start + keyField1Length).trim();

        // Parse fld3Num
        record.fld3Num = Integer.parseInt(line.substring(fld3NumStart, fld3NumStart + fld3NumLength));

        // Parse fld1Num
        record.fld1Num = Integer.parseInt(line.substring(fld1NumStart, fld1NumStart + fld1NumLength));

        // Parse fld2Num
        record.fld2Num = Integer.parseInt(line.substring(fld2NumStart, fld2NumStart + fld2NumLength));

        // Parse fld1Vec
        int fld1VecLength = 24;
        for (int i = 0; i < record.fld1Num; i++) {
            int start = fld1VecStart + (i * fld1VecLength);
            record.fld1Vec.add(line.substring(start, start + fld1VecLength).trim());
        }

        // Parse fld2Vec
        int fld2VecStart = fld1VecStart + (record.fld1Num * fld1VecLength);
        int fld2VecLength = 33;
        for (int i = 0; i < record.fld2Num; i++) {
            int start = fld2VecStart + (i * fld2VecLength);
            record.fld2Vec.add(line.substring(start, start + fld2VecLength).trim());
        }

        // Parse fld3Length and fld3Text
        if (record.fld3Num > 0) {
            int fld3LengthStart = fld2VecStart + (record.fld2Num * fld2VecLength);
            int fld3Length = Integer.parseInt(line.substring(fld3LengthStart, fld3LengthStart + 4));

            int fld3TextStart = fld3LengthStart + 4;
            record.fld3Text = line.substring(fld3TextStart).trim();
        } else {
            record.fld3Text = "";
        }

        return record;
    }
    public static void main(String[] args) {
        try {
            // Parse the copybook to determine field positions
            Map<String, Integer> fieldPositions = parseCopybook("data/last-rec-veriable-copybook");
            System.out.println(fieldPositions);
            // Load the file containing records
            List<String> lines = Files.readAllLines(Paths.get("data/sample_new_records_raw.txt"));

            // Parse each line and print the result
            for (String line : lines) {
                Record record = parseRecord(line);
                System.out.println(record);
            }
        } catch (IOException e) {
            System.err.println("Error reading the file: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Error parsing a record: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
