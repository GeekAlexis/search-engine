package edu.upenn.cis455;

import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis455.utils.ParserWritable;

public class Inverter extends Reducer<Text, ParserWritable, Text, Text> {
    private static final Logger logger = LogManager.getLogger(Inverter.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {}
    
    @Override
    protected void reduce(Text key, Iterable<ParserWritable> values, Context context) throws IOException, InterruptedException {
        Map<Integer, List<Integer>> hitMap = new HashMap<>();
        String term = key.toString();

        for (ParserWritable value : values) {
            // term = value.getTerm();
            int docId = value.getDocId();
            int pos = value.getPos();

            List<Integer> posList = hitMap.get(docId);
            if (posList == null) {
                posList = new ArrayList<>();
                hitMap.put(docId, posList);
            }

            posList.add(pos);
            // Insert to table 3, get generated keys
        }

        int docFreq = hitMap.size();

        for (var entry : hitMap.entrySet()) {
            int docId = entry.getKey();
            List<Integer> posList = entry.getValue();
            
            int termFreq = posList.size();
            context.write(key);
        }
    }
}
