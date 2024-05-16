package com.example.BitCaskHandler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.google.common.primitives.Ints;

public class FileAccess {
    // setting threshold to be 50kbytes
    public static final long sizeThreshold = 50000l;
    public static final String nameFile = "/data/name.txt";
    public static final String segmentDir = "/data/bitcask/";
    public String filePath;
    public File activeFile;
    public FileOutputStream outStream;
    public int currentSegment;
    public int byteOffset;
    public FileAccess() throws IOException{
        File f = new File(nameFile);
        if(f.exists()) { 
            BufferedReader br = new BufferedReader(new FileReader(nameFile));
            this.currentSegment = Integer.parseInt(br.readLine());
            br.close();
        }else{
            f.createNewFile();
            this.currentSegment = 1;
            BufferedWriter bw = new BufferedWriter(new FileWriter(nameFile));
            bw.write(String.valueOf(1));
            bw.close();
        }
        
        this.filePath = segmentDir+"segment-"+this.currentSegment;
        this.activeFile = new File(filePath);
        this.byteOffset = (int)activeFile.length();
        this.outStream = new FileOutputStream(activeFile,true);
    }
    // function to append the new key value pair to currently active segment
    public int[] writeBytes(int key, String value) throws IOException{
        int[] offsets = new int[3];
        offsets[2] = 0;
        if(this.isFull()){
            System.out.println("making new segment to be the current active segment...");
            this.makeNewSegment();
            offsets[2] = 1;
            System.out.println("file created successfully! file name: segment-"+this.currentSegment);
        }
        byte[] valBytes= value.getBytes();
        this.outStream.write(Ints.toByteArray(key));
        this.outStream.write(Ints.toByteArray(valBytes.length));
        this.outStream.write(valBytes);
        // to return the start offset of the record
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
