package com.example;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import com.google.common.primitives.Ints;

public class FileAccess {
    public static final long sizeThreshold = 500l;
    public static final String nameFile = "central_station/src/main/java/com/example/name.txt";
    public static final String segmentDir = "central_station/src/main/java/com/example/bitcask/storage/";
    public String filePath;
    public File activeFile;
    public FileOutputStream outStream;
    public int currentSegment;
    public int byteOffset;
    public FileAccess() throws IOException{
        BufferedReader br = new BufferedReader(new FileReader(nameFile));
        this.currentSegment = Integer.parseInt(br.readLine());
        br.close();
        this.filePath = segmentDir+"segment-"+this.currentSegment;
        this.activeFile = new File(filePath);
        this.byteOffset = (int)activeFile.length();
        this.outStream = new FileOutputStream(activeFile,true);
    }
    // function to append the new key value pair to currently active segment
    public int[] writeBytes(int key, String value) throws IOException{
        if(this.isFull()){
            System.out.println("making new segment to be the current active segment...");
            this.makeNewSegment();
            System.out.println("file created successfully! file name: segment-"+this.currentSegment);
        }
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
    // function to make a new segment if current one is already full
    private void makeNewSegment() throws IOException {
        this.outStream.close();
        this.currentSegment++;
        BufferedWriter bw = new BufferedWriter(new FileWriter(nameFile));
        bw.write(String.valueOf(currentSegment));
        bw.close();
        this.filePath = segmentDir+"segment-"+this.currentSegment;
        this.activeFile = new File(filePath);
        this.byteOffset = 0;
        this.outStream = new FileOutputStream(activeFile,true);
    }
    // function to check if currently active segment is full
    public boolean isFull(){
        return this.activeFile.length() >= sizeThreshold;
    }

    public static String readRecord(Pointer p) throws IOException{
        String filePath = segmentDir+"segment-"+p.ID;
        RandomAccessFile raf = new RandomAccessFile(filePath, "r");
        raf.seek(p.offset);
        byte[] keyArr = new byte[4];
        byte[] valSizeArr = new byte[4];
        byte[] valueArr = new byte[p.length-8];
        raf.read(keyArr);
        raf.read(valSizeArr);
        raf.read(valueArr);
        raf.close();
        int key = ByteBuffer.wrap(keyArr).getInt();
        int valSize = ByteBuffer.wrap(valSizeArr).getInt();
        String value = new String(valueArr);
        return value;
    }
    // this is for testing purposes only
    // public static void main(String[] args) throws IOException {
    //     FileAccess A = new FileAccess();
    //     System.out.println("initially!!!");
    //     System.out.println(A.filePath);
    //     System.out.println(A.currentSegment);
    //     for(int i=0;i<30;i++){
    //         System.out.println("writing values in active:");
    //         int[] offsets = A.writeBytes(2, "This is for testing purposes only 12345678");
    //         System.out.println(offsets[0]+" becomes "+offsets[1]);
    //     }

    //     A.makeNewSegment();
    //     System.out.println("After 1 call to makenewsegment!!!");
    //     System.out.println(A.filePath);
    //     System.out.println(A.currentSegment);

    //     System.out.println("reading record");
    //     System.out.println(A.readRecord(new Pointer(0, 0, 50)));

    // }


}
