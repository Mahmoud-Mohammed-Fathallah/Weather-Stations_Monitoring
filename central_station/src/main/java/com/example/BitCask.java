package com.example;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.primitives.Ints;
import java.util.concurrent.locks.Lock; 
import java.util.concurrent.locks.ReadWriteLock; 
import java.util.concurrent.locks.ReentrantReadWriteLock; 

public class BitCask {
    public static final String outputDir = "central_station/src/main/java/com/example/bitcask/storage/";
    public final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(); 
    public final Lock writeLock = readWriteLock.writeLock(); 
    public final Lock readLock = readWriteLock.readLock();
    public ConcurrentHashMap<Integer,Pointer> keyDir;
    public FileAccess fileAccess;
    public BitCask() throws IOException{
        keyDir = new ConcurrentHashMap<Integer,Pointer>();
        File f = new File(outputDir+"hintfile");
        if(f.exists()){
            System.out.println("recovering from hint file...");
            recoverFromHintFile(f);
            System.out.println("recovered from hint file successfully!");
        }else{
            System.out.println("No hint file to recover from, starting from scratch!");
        }
        this.fileAccess = new FileAccess();
        // code for compaction task scheduling
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        Runnable periodicTask = new Runnable() {
            public void run() {
                try {
                    startCompaction();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        executor.scheduleAtFixedRate(periodicTask, 3, 3, TimeUnit.SECONDS);
        // *************************************************************************
        // testing the reading concurrently with compaction process
        ScheduledExecutorService executor2 = Executors.newSingleThreadScheduledExecutor();
        Runnable periodicTask2 = new Runnable() {
            public void run() {
                try {
                    testing();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        executor2.scheduleAtFixedRate(periodicTask2, 3, 3, TimeUnit.SECONDS);
        // *************************************************************************
    }
    
    public void recoverFromHintFile(File hint) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(hint, "r");
        raf.seek(0);
        writeLock.lock();
        while(raf.length()-raf.getFilePointer() >= 16 ){
            byte[] keyArr = new byte[4];
            byte[] idArr = new byte[4];
            byte[] offsetArr = new byte[4];
            byte[] lengthArr = new byte[4];
            raf.read(keyArr);
            raf.read(idArr);
            raf.read(offsetArr);
            raf.read(lengthArr);
            int key = ByteBuffer.wrap(keyArr).getInt();
            int id = ByteBuffer.wrap(idArr).getInt();
            int offset = ByteBuffer.wrap(offsetArr).getInt();
            int length = ByteBuffer.wrap(lengthArr).getInt();
            this.keyDir.put(key, new Pointer(id, offset, length));

        }
        writeLock.unlock();
        raf.close();
    }

    public static void writeHintFile(ConcurrentHashMap<Integer,Pointer> keyDir) throws IOException{
        FileOutputStream fos = new FileOutputStream(outputDir+"hintfile",false);
        for(int k : keyDir.keySet()){
            fos.write(Ints.toByteArray(k));
            Pointer p = keyDir.get(k);
            fos.write(Ints.toByteArray(p.ID));
            fos.write(Ints.toByteArray(p.offset));
            fos.write(Ints.toByteArray(p.length));
        }
        fos.close();
    }

    public String readRecordForKey(int key) throws IOException{
        readLock.lock();
        Pointer recordPointer = this.keyDir.get(key);
        if(recordPointer == null){
            System.out.println("key does not exist!!");
            return null;
        }
        String record = FileAccess.readRecord(recordPointer);
        readLock.unlock();
        return record;
    }


    public void writeRecordToActiveFile(int key, String value) throws IOException{
        writeLock.lock();
        int[] offsets = this.fileAccess.writeBytes(key, value);
        this.keyDir.put(key,new Pointer(this.fileAccess.currentSegment,offsets[0],offsets[1]-offsets[0]));
        writeLock.unlock();

    }

    public void close() throws IOException{
        readLock.lock();
        writeHintFile(this.keyDir);
        readLock.unlock();
    }

    public void startCompaction() throws IOException{
        Comaction c = new Comaction(this.keyDir,readLock,writeLock);
        c.compact();
    }
// *************************************************************************
    // this is for testing purposes only
    public void testing() throws IOException{
        System.out.println(this.readRecordForKey(0));
    }
// *************************************************************************
    public static void main(String[] args) throws IOException {
        BitCask bc = new BitCask();
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 80; i++) {
                bc.writeRecordToActiveFile(j,"this is latest value for key " + j +" the value is "+i);
            }
        }
        
        // bc.close();
        // System.out.println(bc.keyDir.get(0).ID);
        // System.out.println(bc.readRecordForKey(0));
        // bc.startCompaction();
        bc.close();
        
    }

}
