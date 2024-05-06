package com.example;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.primitives.Ints;

public class BitCask {
    public static final String outputDir = "src/main/java/com/example/bitcask/storage/";
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
    }
    
    public void recoverFromHintFile(File hint) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(hint, "r");
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
    }

    public void writeHintFile() throws IOException{
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
        Pointer recordPointer = this.keyDir.get(key);
        if(recordPointer == null){
            System.out.println("key does not exist!!");
            return null;
        }
        String record = FileAccess.readRecord(recordPointer);
        return record;
    }


    public void writeRecordToActiveFile(int key, String value) throws IOException{
        int[] offsets = this.fileAccess.writeBytes(key, value);
        this.keyDir.put(key,new Pointer(this.fileAccess.currentSegment,offsets[0],offsets[1]-offsets[0]));

    }

    public void close() throws IOException{
        this.writeHintFile();
    }

    public void startCompaction() throws IOException{
        Comaction c = new Comaction(this.keyDir);
        c.compact();
    }
    public static void main(String[] args) throws IOException {
        BitCask bc = new BitCask();
        // for (int j = 0; j < 10; j++) {
        //     for (int i = 0; i < 10; i++) {
        //         bc.writeRecordToActiveFile(j,"this is latest value for key " + j +" the value is "+i);
        //     }
        // }
        
        // bc.close();
        // System.out.println(bc.keyDir.get(0).ID);
        // System.out.println(bc.readRecordForKey(0));
        bc.startCompaction();
        bc.close();
    }

}
