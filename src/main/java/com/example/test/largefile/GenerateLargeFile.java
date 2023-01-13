package com.example.test.largefile;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class GenerateLargeFile {
    private static final int ITERATIONS = 1;
    private static final double MEGA = (Math.pow(1024, 2));
    private static final double GIGA = (Math.pow(1024, 3));
    private static final int RECORD_COUNT = 25000000;
    private static final String RECORD = "id,country,city,temperature\n";
    private static final int RECORD_SIZE = RECORD.getBytes().length;

    public static void main(String[] args) throws Exception {
        startWriting(1 * GIGA);
        startWriting(5 * GIGA);
        startWriting(10 * GIGA);
    }

    private static void startWriting(double fileSizeRequired) throws IOException {
        List<String> records = new ArrayList<>(RECORD_COUNT);
        double size = 0;
        while (size <= fileSizeRequired) {
            records.add(RECORD);
            size += RECORD_SIZE;
        }
        System.out.print("\n"+String.format("%.0f", size / GIGA) + " GB");

        String fileName = String.format("%.0f", fileSizeRequired / GIGA) + "-Gb.txt";

        for (int i = 0; i < ITERATIONS; i++) {
            System.out.println("\nIteration " + i);
            //raw without buffer
            writeRaw(records, fileName);
            //8192 byte buffer
            writeBuffered(records, 8192, fileName);
            //4MB buffer
            writeBuffered(records, 4 * (int) MEGA, fileName);
        }
    }

    private static void writeRaw(List<String> records, String fileName) throws IOException {
        //File file = File.createTempFile("foo", ".txt");
        File file = new File("src/main/resources/" + fileName);
        try {
            FileWriter writer = new FileWriter(file);
            System.out.print("Writing raw... ");
            write(records, writer);
        } finally {
            // comment this out if you want to inspect the files afterward
            //file.delete();
        }
    }

    private static void writeBuffered(List<String> records, int bufSize, String fileName) throws IOException {
        File file = new File("src/main/resources/" + fileName);
        try {
            FileWriter writer = new FileWriter(file);
            BufferedWriter bufferedWriter = new BufferedWriter(writer, bufSize);

            System.out.print("Writing buffered (buffer size: " + bufSize + ")... ");
            write(records, bufferedWriter);
        } finally {
            // comment this out if you want to inspect the files afterward
            //file.delete();
        }
    }

    private static void write(List<String> records, Writer writer) throws IOException {
        long start = System.currentTimeMillis();
        for (String record : records) {
            writer.write(record);
        }
        // writer.flush(); // close() should take care of this
        writer.close();
        long end = System.currentTimeMillis();
        System.out.println((end - start) / 1000f + " seconds");
    }
}
