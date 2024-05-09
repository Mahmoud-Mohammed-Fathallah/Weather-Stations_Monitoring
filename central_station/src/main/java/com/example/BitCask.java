package com.example;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
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
        File directory = new File(outputDir);
        String[] names = directory.list();
        if(names.length!=0){
            System.out.println("recovering from hint file...");
            recoverFromHintFile(directory);
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
        executor.scheduleAtFixedRate(periodicTask, 120, 120, TimeUnit.SECONDS);
    }
    
    public void recoverFromHintFile(File directory) throws IOException {
        ArrayList<String> hintFiles = new ArrayList<>(),segmentFiles = new ArrayList<>();
        for(String f : directory.list()){
            if(f.contains("hint")){
                hintFiles.add(f);
            }else if(f.contains("segment")){
                segmentFiles.add(f);
            }
        }
        Collections.sort(hintFiles,Collections.reverseOrder());
        Collections.sort(segmentFiles);
        writeLock.lock();
        if(hintFiles.size()>0){
            String latestHint = hintFiles.get(0);
            System.out.println("reading file "+latestHint);
            RandomAccessFile raf = new RandomAccessFile(new File(outputDir+latestHint), "r");
            raf.seek(0);
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
            raf.close();
            recoverFromSegments(latestHint,segmentFiles);
        }else{
            recoverFromSegments(null,segmentFiles);
        }
        writeLock.unlock();

    }

    private void recoverFromSegments(String latestHint, ArrayList<String> segmentFiles) throws IOException {
        if(latestHint != null){
            int hintNum = Integer.parseInt(latestHint.substring(9));
            for(String s : segmentFiles){
                int segNum = Integer.parseInt(s.substring(8));
                if(segNum > hintNum){
                    System.out.println("reading from file "+s);
                    recoverFromSegment(s,segNum);
                }
            }
        }else{
            for(String s : segmentFiles){
                System.out.println("reading from file "+s);
                int segNum = Integer.parseInt(s.substring(8));
                recoverFromSegment(s, segNum);
            }
        }
    }

    private void recoverFromSegment(String fileName,int segNum) throws IOException {
        String filePath = outputDir+fileName;
        RandomAccessFile raf = new RandomAccessFile(filePath, "r");
        int byteOffset = 0;
        raf.seek(0);
        while(byteOffset < raf.length()){
            byte[] keyArr = new byte[4];
            byte[] valSizeArr = new byte[4];
            raf.read(keyArr);
            raf.read(valSizeArr);
            int key = ByteBuffer.wrap(keyArr).getInt();
            int valSize = ByteBuffer.wrap(valSizeArr).getInt();
            byte[] valueArr = new byte[valSize];
            raf.read(valueArr);
            this.keyDir.put(key, new Pointer(segNum, byteOffset, byteOffset + 8 + valSize));
            byteOffset = byteOffset + 8 + valSize;
        }
        raf.close();
    }

    public static void writeHintFile(ConcurrentHashMap<Integer,Pointer> keyDir, String fileName) throws IOException{
        FileOutputStream fos = new FileOutputStream(outputDir+fileName,false);
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
        // if new active file is made write current keydir to hintfile with number of previous active segment
        if(offsets[2] == 1){
            writeHintFile(keyDir, "hintfile-"+(this.fileAccess.currentSegment-1));
        }
        writeLock.unlock();

    }

    public void close() throws IOException{
        readLock.lock();
        writeHintFile(this.keyDir,"hintfile-"+this.fileAccess.currentSegment);
        readLock.unlock();
    }

    public void startCompaction() throws IOException{
        Comaction c = new Comaction(this.keyDir,readLock,writeLock);
        c.compact();
    }

}
