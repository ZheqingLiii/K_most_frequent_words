package com.company;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class Main_multithread_hashmap_one_heap {
    private static PriorityQueue<Pair> queue = new PriorityQueue<>(Comparator.comparing(Pair->Pair.count));
    private static HashMap<String, Integer> g_hashWords = new HashMap<String, Integer>();
    private static Instant startTime;
    private static Integer num = 0;
    private static Integer fileNum;

    private static Multi_threading_map.ResultListener listener = new Multi_threading_map.ResultListener() {
        public synchronized void result(HashMap<String, Integer> hashWords) {

            //merge
            for(Map.Entry<String, Integer> word: hashWords.entrySet()) {
                Pair p = new Pair(word.getKey(), word.getValue());
                g_hashWords.merge(p.getStr(), p.getCount(), (x, y) -> x + y);
            }

            num++;
            System.out.println(num);

            if (num.equals(fileNum)) {
                //put all values into heap
                for(Map.Entry<String, Integer> word: g_hashWords.entrySet()) {
                    Pair p = new Pair(word.getKey(), word.getValue());
                    queue.offer(p);
                    if (queue.size() > 100) {
                        queue.poll();
                    }

                }

                //print heap
                Pair tmp;
                while (!queue.isEmpty()) {
                    tmp = queue.poll();
                    System.out.println(tmp.getStr() + " " + tmp.getCount());
                }

                //print time
                Instant stopTime = Instant.now();
                Duration interval = Duration.between(startTime, stopTime);
                System.out.println("Execution time: " + interval.getSeconds() + " sec");
            }


        }
    };

    public static void main(String[] args) throws IOException {
        startTime = Instant.now();

        //split file, return number of files
        fileNum = Filesplit.splitFile(new File("/users/lizheqing/Desktop/DataSet/data_1GB.txt"), 130); //8GB:1050  1GB:130
        System.out.println(fileNum);

        for (int i= 1; i <= fileNum; i++) {
            Runnable word_count1 = new Multi_threading_map("/users/lizheqing/Desktop/DataSet/" + i + "_data_1GB.txt", listener);

            Thread t = new Thread(word_count1);
            t.start();

            /* this will wait for the thread to finish before running the next
            try {
                t.join();
            } catch (Exception e) {
                System.out.println("Exception is been caught: "+e);
            }

             */
        }

    }
}

