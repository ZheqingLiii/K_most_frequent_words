package com.company;

import java.util.*;

public class Multi_threading implements Runnable {
    private String filename;
    private ResultListener listener;

    Multi_threading(String filename, ResultListener listener) {
        this.filename = filename;
        this.listener = listener;
    }

    //result interface
    public interface ResultListener {
        void result(TreeMap<String, Integer> map);
    }

    public void run() {
        try {
            //store all words in a hash map
            HashMap<String, Integer> words = new HashMap<String, Integer>();
            ValueComparator bvc = new ValueComparator((words));
            TreeMap<String, Integer> sorted_words = new TreeMap<String, Integer>(bvc);

            Hash_words.getWords(filename, words);
            sorted_words.putAll(words);

            listener.result(sorted_words);
        }
        catch (Exception e) {
            System.out.println("Exception is caught");
        }
    }





}
