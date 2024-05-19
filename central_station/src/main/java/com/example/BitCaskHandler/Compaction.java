package com.example.BitCaskHandler;

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

import com.google.common.primitives.Ints;

public class Compaction {
    public static final String nameFile = "/data/name.txt";
    public static final String segmentDir = "/data/bitcask/";
    public Lock readLock;
    public Lock writeLock;
    public int activeFile;
    public FileOutputStream outStream;
    public int byteOffset;
    public ConcurrentHashMap<Integer,Pointer> keyDir;
    public ConcurrentHashMap<Integer,Pointer> privateKeyDir;
    public HashMap<Integer,String> mostUpdated;
    public Compaction(ConcurrentHashMap<Integer,Pointer> map,Lock read,Lock write) throws IOException{
        this.readLock = read;
        this.writeLock = write;
        // getting the name of current active file
        BufferedReader br = new BufferedReader(new FileReader(nameFile));
        this.activeFile =  Integer.parseInt(br.readLine());
        System.out.println("****** ======>>>>> Active file is:"+activeFile);
        br.close();
        this.keyDir = map;
        this.privateKeyDir = new ConcurrentHashMap<>();
        this.privateKeyDir.putAll(map);
        this.outStream = new FileOutputStream(new File(segmentDir+"compacted"),false);
        this.byteOffset = 0;
        this.mostUpdated = new HashMap<>();
    }
    public void compact() throws IOException{
        System.out.println("compaction process starting...");
        for(Entry<Integer,Pointer>  e : this.privateKeyDir.entrySet()){
            if(e.getValue().ID >= this.activeFile){
                continue;
            }
            String mostUpdatedValue = FileAccess.readRecord(e.getValue());
            this.mostUpdated.put(e.getKey(), mostUpdatedValue);
        }
        this.writeMostUpdated();
        this.outStream.close();
        this.updateStorage();
        System.out.println("finished compaction process successfully!!!");
    }
    private void writeMostUpdated() throws IOException {
        System.out.println("writing compacted file...");
        for(Entry<Integer,String>  e : this.mostUpdated.entrySet()){
            System.out.println("******** writing record  in file  ");
            int[] offsets = this.writeBytes(e.getKey(), e.getValue());
            this.privateKeyDir.put(e.getKey(), new Pointer(0, offsets[0], offsets[1]-offsets[0]));
        }
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
        this.keyDir.putAll(this.privateKeyDir);
        File dir = new File(segmentDir);
        Set<String> fileNames =  Stream.of(dir.listFiles()).filter(file -> !file.isDirectory()).map(File::getName).collect(Collectors.toSet());
        // deleting all files except for active segment and comacted file
        for(String f : fileNames){
            if(f.contains("segment")){
                int segNum = Integer.parseInt(f.substring(8));
                if(segNum >= this.activeFile){
                    continue;
                }
            }
            if(!f.equals("compacted")){
                new File(segmentDir + f).delete();
            }
        }
        // renaming the comacted file to be segment 0
        new File(segmentDir + "compacted").renameTo(new File(segmentDir+"segment-0"));
        BitCask.writeHintFile(keyDir,"hintfile-0");
        System.out.println("storage updated successfully!!");
        this.writeLock.unlock();
    }

}
