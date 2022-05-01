package edu.upenn.cis455.utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ParserGroupingComparator extends WritableComparator {

    public ParserGroupingComparator() {
        super(ParserWritable.class, true);
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        
        ParserWritable key1 = (ParserWritable) wc1;
        ParserWritable key2 = (ParserWritable) wc2;
        return key1.getTerm().compareTo(key2.getTerm());
    }
}