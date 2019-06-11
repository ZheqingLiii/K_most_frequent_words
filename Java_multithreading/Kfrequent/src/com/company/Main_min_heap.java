package com.company;

import java.io.File;
import java.io.FileNotFoundException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class Main_min_heap {
    //store all words in a hash table
    static void getWords(String fileName, Map<String, Integer> words) throws FileNotFoundException {
        Scanner file = new Scanner(new File(fileName));
        while (file.hasNext()) {
            String word = file.next();
            Integer count = words.get(word);
            if(count != null) count++;
            else count = 1;
            words.put(word, count);
        }
        file.close();
    }

    public static void main(String[] args) throws FileNotFoundException {
        Instant startTime = Instant.now();
        HashMap<String, Integer> words = new HashMap<String, Integer>();
        PriorityQueue<Pair> queue = new PriorityQueue<>(Comparator.comparing(Pair->Pair.count));
        getWords("/users/lizheqing/Desktop/DataSet/data_8GB.txt", words);

        //maintain a min heap of size 100
        for(Map.Entry<String, Integer> word: words.entrySet()) {
            Pair p = new Pair(word.getKey(), word.getValue());
            queue.offer(p);
            if (queue.size() > 100) {
                queue.poll();
            }
        }

        //print output
        Pair tmp;
        while (!queue.isEmpty()) {
            tmp = queue.poll();
            System.out.println(tmp.getStr()+ " "+tmp.getCount());
        }

        //print time
        Instant stopTime = Instant.now();
        Duration interval = Duration.between(startTime, stopTime);
        System.out.println("Execution time: " + interval.getSeconds() + " sec");
    }
}

