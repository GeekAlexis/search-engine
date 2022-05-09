package edu.upenn.cis.cis455.utils;

import org.apache.hadoop.mapreduce.Partitioner;

public class ParserPartitioner extends Partitioner<ParserWritable, ParserWritable> {

    @Override
    public int getPartition(ParserWritable key, ParserWritable value, int numPartitions) {
        // // Partition by first character of each term
        // return key.getTerm().charAt(0) % numPartitions;
        return (key.getTerm().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
    
}
