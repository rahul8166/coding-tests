package com.example.test.largefile;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class LargeFileChunk implements Runnable{
    private final FileChannel _channel;
    private final long _startLocation;
    private final int _size;
    int _sequence_number;

    public LargeFileChunk(long loc, int size, FileChannel chnl, int sequence) {
        _startLocation = loc;
        _size = size;
        _channel = chnl;
        _sequence_number = sequence;
    }

    @Override
    public void run() {
        try {
            System.out.println("Reading the channel: " + _startLocation + ":" + _size);
            //allocate memory
            ByteBuffer buff = ByteBuffer.allocate(_size);
            //Read file chunk to RAM
            _channel.read(buff, _startLocation);
            //chunk to String
            String string_chunk = new String(buff.array(), StandardCharsets.UTF_8);
            System.out.println("Done Reading the channel: " + _startLocation + ":" + _size);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
