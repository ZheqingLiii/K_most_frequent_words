package com.company;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Scanner;

public class Hash_words {
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
}
