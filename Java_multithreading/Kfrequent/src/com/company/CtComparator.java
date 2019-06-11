package com.company;

import java.util.Comparator;

// comparator used to define max heap
public class CtComparator implements Comparator<Pair> {

    @Override
    public int compare(Pair p1, Pair p2){
        if (p1.getCount() > p2.getCount()) {
        //if (p1.getCount() <= p2.getCount()) { //min heap
            return -1;
        } else {
            return 1;
        }
    }
}
