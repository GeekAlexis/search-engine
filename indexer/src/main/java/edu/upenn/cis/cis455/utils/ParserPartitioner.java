package edu.upenn.cis.cis455.utils;

import org.apache.hadoop.mapreduce.Partitioner;

public class ParserPartitioner extends Partitioner<ParserWritable, ParserWritable> {

    @Override
    public int getPartition(ParserWritable key, ParserWritable value, int numPartitions) {
        // return Math.abs(key.getTerm().hashCode() % numPartitions);
        // Partition by first character of each term
        return key.getTerm().charAt(0) % numPartitions;
    }
    
}
