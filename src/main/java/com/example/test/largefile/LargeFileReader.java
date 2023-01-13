package com.example.test.largefile;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.toIntExact;

public class LargeFileReader {

    private static final int NUM_OF_THREADS = 8;

    public static void main(String[] args) throws Exception {
        LargeFileReader largeFileReader = new LargeFileReader();
        largeFileReader.read("1-Gb.txt");
        largeFileReader.read("5-Gb.txt");
        largeFileReader.read("10-Gb.txt");
    }

    private void read(String fileName) throws IOException {
        String filePath = "src/main/resources/"+fileName;
        long fileSizeInGB = Files.size(Paths.get(filePath)) / 1024 / 1024 / 1024;

        FileInputStream fileInputStream = new FileInputStream(filePath);
        FileChannel channel = fileInputStream.getChannel();
        long remaining_size = channel.size(); //get the total number of bytes in the file
        long chunk_size = remaining_size / NUM_OF_THREADS; //file_size/threads
        //Max allocation size allowed is ~2GB
        if (chunk_size > (Integer.MAX_VALUE - 5)) {
            chunk_size = (Integer.MAX_VALUE - 5);
        }

        Instant startInstant = Instant.now();

        //thread pool
        ExecutorService executor = Executors.newFixedThreadPool(NUM_OF_THREADS);
        long start_loc = 0;//file pointer
        int i = 0; //loop counter
        while (remaining_size >= chunk_size) {
            //launches a new thread
            executor.execute(new LargeFileChunk(start_loc, toIntExact(chunk_size), channel, i));
            remaining_size = remaining_size - chunk_size;
            start_loc = start_loc + chunk_size;
            i++;
        }
        //load the last remaining piece
        executor.execute(new LargeFileChunk(start_loc, toIntExact(remaining_size), channel, i));
        //Tear Down
        executor.shutdown();
        //Wait for all threads to finish
        while (!executor.isTerminated()) {
            //wait for infinity time
        }
        System.out.println("Finished all threads");
        fileInputStream.close();
        Duration d = Duration.between(startInstant, Instant.now());
        long durationInMs = TimeUnit.NANOSECONDS.toMillis(d.getNano());
        System.out.println("Total time taken to read " + fileSizeInGB + " Gb with " + NUM_OF_THREADS + " threads - " + durationInMs + " ms");
    }

}