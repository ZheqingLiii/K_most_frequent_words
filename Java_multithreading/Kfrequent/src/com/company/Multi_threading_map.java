package com.company;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class Multi_threading_map implements Runnable {
    private String filename;
    private ResultListener listener;

    Multi_threading_map(String filename, ResultListener listener) {
        this.filename = filename;
        this.listener = listener;
    }

    //result interface
    public interface ResultListener {
        void result(HashMap<String, Integer> words);
    }

    public void run() {
        try {
            //store all words in a hash map
            HashMap<String, Integer> words = new HashMap<String, Integer>();
            Hash_words.getWords(filename, words);
            listener.result(words);
        }
        catch (Exception e) {
            System.out.println("Exception is caught");
        }
    }


}
