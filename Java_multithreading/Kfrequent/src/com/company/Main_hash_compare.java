package com.company;

import java.io.File;
import java.io.FileNotFoundException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

public class Main_hash_compare {

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
        ValueComparator bvc = new ValueComparator((words));
        TreeMap<String, Integer> sorted_words = new TreeMap<String, Integer>(bvc);

        //store all words in a hash table
        getWords("/users/lizheqing/Desktop/DataSet/data_8GB.txt", words);

        //move into treeMap
        sorted_words.putAll(words);

        //get time
        Instant stopTime = Instant.now();
        Duration interval = Duration.between(startTime, stopTime);
        System.out.println("Execution time using comparator: " + interval.getSeconds() + " sec");

        //print output
        int i = 0;
        for (Map.Entry<String, Integer> word: sorted_words.entrySet()) {
            if (i >= 100) break;
            System.out.println(word);
            i++;
        }
    }
}

