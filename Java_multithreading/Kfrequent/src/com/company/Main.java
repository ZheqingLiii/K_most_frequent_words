package com.company;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

class Pair {
    private String str;
    int count;
    public Pair(String str, Integer count) {
        this.str = str;
        this.count = count;
    }
    public String getStr() {
        return str;
    }
    public Integer getCount() {
        return count;
    }
}

public class Main {
    private static TreeMap<String, Integer> g_map = new TreeMap<String, Integer>();
    private static Instant startTime;
    private static Integer num = 0;
    private static Integer fileNum;

    private static Multi_threading.ResultListener listener = new Multi_threading.ResultListener() {
        public synchronized void result(TreeMap<String, Integer> returnedMap) {

            if(g_map.isEmpty()) { //spacial case for the first one
                for (Map.Entry<String, Integer> word: returnedMap.entrySet()) {
                    g_map.put(word.getKey(), word.getValue());
                    if (g_map.size() >= 200) break;
                }
            }
            else {
                int i = 0;
                //merge
                for (Map.Entry<String, Integer> word: returnedMap.entrySet()) {
                    i++;
                    if (g_map.containsKey(word.getKey())) {
                        g_map.merge(word.getKey(), word.getValue(), (x, y) -> x+y);
                    } else {
                        g_map.put(word.getKey(), word.getValue());
                    }
                    if (i >= 120) break;
                }
            }

            num++;
            System.out.println(num);
            //output
            if (num.equals(fileNum+1)) {
                //put everything from treemap to heap
                PriorityQueue<Pair> q = new PriorityQueue<>(Comparator.comparing(Pair->Pair.count));
                for (Map.Entry<String, Integer> word: g_map.entrySet()) {
                    Pair p = new Pair(word.getKey(), word.getValue());
                    q.offer(p);
                    if (q.size() > 100) {
                        q.poll();
                    }
                }

                //print heap
                Pair tmp;
                while(!q.isEmpty()) {
                    tmp = q.poll();
                    System.out.println(tmp.getStr() + "  " + tmp.getCount());
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
        int fileNum = Filesplit.splitFile(new File("/users/lizheqing/Desktop/DataSet/data_32GB.txt"), 130);
        System.out.println(fileNum);

        int i = 0;
        //this is an example of having 8 threads running in parallel
        while (i < 256) {
            ++i;
            Runnable word_count1 = new Multi_threading("/users/lizheqing/Desktop/DataSet/" + i + "_data_32GB.txt", listener);
            ++i;
            Runnable word_count2 = new Multi_threading("/users/lizheqing/Desktop/DataSet/" + i + "_data_32GB.txt", listener);
            ++i;
            Runnable word_count3 = new Multi_threading("/users/lizheqing/Desktop/DataSet/" + i + "_data_32GB.txt", listener);
            ++i;
            Runnable word_count4 = new Multi_threading("/users/lizheqing/Desktop/DataSet/" + i + "_data_32GB.txt", listener);
            ++i;
            Runnable word_count5 = new Multi_threading("/users/lizheqing/Desktop/DataSet/" + i + "_data_32GB.txt", listener);
            ++i;
            Runnable word_count6 = new Multi_threading("/users/lizheqing/Desktop/DataSet/" + i + "_data_32GB.txt", listener);
            ++i;
            Runnable word_count7 = new Multi_threading("/users/lizheqing/Desktop/DataSet/" + i + "_data_32GB.txt", listener);
            ++i;
            Runnable word_count8 = new Multi_threading("/users/lizheqing/Desktop/DataSet/" + i + "_data_32GB.txt", listener);


            Thread t1 = new Thread(word_count1);
            Thread t2 = new Thread(word_count2);
            Thread t3 = new Thread(word_count3);
            Thread t4 = new Thread(word_count4);
            Thread t5 = new Thread(word_count5);
            Thread t6 = new Thread(word_count6);
            Thread t7 = new Thread(word_count7);
            Thread t8 = new Thread(word_count8);
            t1.start();
            t2.start();
            t3.start();
            t4.start();
            t5.start();
            t6.start();
            t7.start();
            t8.start();

            try {
                t1.join();
                t2.join();
                t3.join();
                t4.join();
                t5.join();
                t6.join();
                t7.join();
                t8.join();
            } catch (Exception e) {
                System.out.println("Exception is been caught: " + e);
            }
        }
        //actually there is one more file left
        Runnable word_count_l = new Multi_threading("/users/lizheqing/Desktop/DataSet/257_data_32GB.txt", listener);
        Thread t_l = new Thread(word_count_l);
        t_l.start();
    }
}

