package com.company;

import java.io.*;
import java.time.Duration;
import java.time.Instant;

public class Filesplit {
    //split file into chunks of small files
    public static int splitFile(File f, Integer size) throws IOException {

        int partCounter = 1;

        int sizeOfFiles = 1024 * 1024 * size;// 1MB * size
        byte[] buffer = new byte[sizeOfFiles];

        String fileName = f.getName();

        //try-with-resources to ensure closing stream
        try (FileInputStream fis = new FileInputStream(f);
             BufferedInputStream bis = new BufferedInputStream(fis)) {

            int bytesAmount = 0;
            while ((bytesAmount = bis.read(buffer)) > 0) {
                //write each chunk of data into separate file with different number in name
                String filePartName = String.format("%d_%s", partCounter++, fileName);
                //String filePartName = partCounter+"_"+fileName;
                File newFile = new File(f.getParent(), filePartName);
                try (FileOutputStream out = new FileOutputStream(newFile)) {
                    out.write(buffer, 0, bytesAmount);
                }
            }
        }
        return partCounter-1;
    }

    public static void main(String[] args) throws IOException {
        Instant startTime = Instant.now();
        splitFile(new File("/users/lizheqing/Desktop/DataSet/data_32GB.txt"), 128);
        Instant stopTime = Instant.now();
        Duration interval = Duration.between(startTime, stopTime);
        System.out.println("Execution time: " + interval.getSeconds() + " sec");
    }
}
