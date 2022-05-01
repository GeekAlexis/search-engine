package edu.upenn.cis455.utils;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ParserPartitioner extends Partitioner<ParserWritable, NullWritable> {

    @Override
    public int getPartition(ParserWritable key, NullWritable value, int numPartitions) {
        // return Math.abs(key.getTerm().hashCode() % numPartitions);
        // Partition by first character of each term
        return key.getTerm().charAt(0) % numPartitions;
    }
    
}
