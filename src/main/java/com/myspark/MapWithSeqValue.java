package com.myspark;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Key is string, value is sequence
 * @author shijie
 * @link http://stackoverflow.com/questions/81346/most-efficient-way-to-increment-a-map-value-in-java
 */
public class MapWithSeqValue {

    private Map<String, Integer> freq = new LinkedHashMap<String, Integer>();
    private Integer seq = 0;
    
    public Integer add(String word) {
        Integer count = freq.get(word);
        if (count == null) {
            count = ++seq;
            freq.put(word, count);
        }
        return count;
    }
    
    public Integer get(String word) {
        return freq.get(word);
    }
    
    public String getKeyByValue(Integer i) {
        if (i == null) {
            return null;
        } else {
            Set<Map.Entry<String,Integer>> set =freq.entrySet();
            for (Map.Entry<String,Integer> entry : set) {
                if (i.equals(entry.getValue())) {
                    return entry.getKey();
                }
            }
            return null;
        }
    }
    
    public String toString() {
        return freq.toString();
    }
    
    public static void main(String[] args) {
        MapWithSeqValue mayWithSeq = new MapWithSeqValue();
        
        mayWithSeq.add("Value1");
        mayWithSeq.add("Value2");
        mayWithSeq.add("Value3");
        
        System.out.println(mayWithSeq);
        System.out.println(mayWithSeq.getKeyByValue(2));
    }
}
