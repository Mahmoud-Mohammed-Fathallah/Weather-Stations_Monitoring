package com.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.concurrent.locks.Lock; 
import java.util.concurrent.locks.ReadWriteLock; 
import java.util.concurrent.locks.ReentrantReadWriteLock; 

import com.google.common.primitives.Ints;

public class Comaction {
    public static final String nameFile = "central_station/src/main/java/com/example/name.txt";
    public static final String segmentDir = "central_station/src/main/java/com/example/bitcask/storage/";
    public Lock readLock;
    public Lock writeLock;
    public int activeFile;
    public FileOutputStream outStream;
    public int byteOffset;
    public ConcurrentHashMap<Integer,Pointer> keyDir;
    public HashMap<Integer,String> mostUpdated;
    public Comaction(ConcurrentHashMap<Integer,Pointer> map,Lock read,Lock write) throws IOException{
        this.readLock = read;
        this.writeLock = write;
        // getting the name of current active file
        BufferedReader br = new BufferedReader(new FileReader(nameFile));
        this.activeFile =  Integer.parseInt(br.readLine());
        br.close();
        this.keyDir = map;
        this.outStream = new FileOutputStream(new File(segmentDir+"compacted"),false);
        this.byteOffset = 0;
        this.mostUpdated = new HashMap<>();
    }
    public void compact() throws IOException{
        this.readLock.lock();
        System.out.println("compaction process starting...");
        for(Entry<Integer,Pointer>  e : this.keyDir.entrySet()){
            if(e.getValue().ID >= this.activeFile){
                continue;
            }
            String mostUpdatedValue = FileAccess.readRecord(e.getValue());
            this.mostUpdated.put(e.getKey(), mostUpdatedValue);
        }
        this.readLock.unlock();
        this.writeMostUpdated();
        this.outStream.close();
        this.updateStorage();
        System.out.println("finished compaction process successfully!!!");
    }
    private void writeMostUpdated() throws IOException {
        this.writeLock.lock();
        System.out.println("writing compacted file...");
        for(Entry<Integer,String>  e : this.mostUpdated.entrySet()){
            System.out.println("******** writing record  in file  ");
            int[] offsets = this.writeBytes(e.getKey(), e.getValue());
            this.keyDir.put(e.getKey(), new Pointer(0, offsets[0], offsets[1]-offsets[0]));
        }
        this.writeLock.unlock();
        System.out.println("finished writing compacted file!");
    }
    public int[] writeBytes(int key, String value) throws IOException{
        byte[] valBytes= value.getBytes();
        this.outStream.write(Ints.toByteArray(key));
        this.outStream.write(Ints.toByteArray(valBytes.length));
        this.outStream.write(valBytes);
        // to return the start offset of the record
        int[] offsets = new int[2];
        offsets[0] = this.byteOffset;
        this.byteOffset += 8 + valBytes.length;
        offsets[1] = this.byteOffset;
        return offsets;
    }
    public void updateStorage() throws IOException{
        this.writeLock.lock();
        System.out.println("updating storage...");
        File dir = new File(segmentDir);
        Set<String> fileNames =  Stream.of(dir.listFiles()).filter(file -> !file.isDirectory()).map(File::getName).collect(Collectors.toSet());
        // deleting all files except for active segment and comacted file
        for(String f : fileNames){
            if(!f.equals("segment-"+this.activeFile) && !f.equals("compacted") && !f.equals("hintfile")){
                new File(segmentDir + f).delete();
            }
        }
        // renaming the comacted file to be segment 0
        new File(segmentDir + "compacted").renameTo(new File(segmentDir+"segment-0"));
        BitCask.writeHintFile(keyDir);
        System.out.println("storage updated successfully!!");
        this.writeLock.unlock();
    }

}
