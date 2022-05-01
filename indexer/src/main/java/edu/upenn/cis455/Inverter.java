package edu.upenn.cis455;

import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis455.storage.Storage;
import edu.upenn.cis455.storage.StorageFactory;
import edu.upenn.cis455.storage.HitList;
import edu.upenn.cis455.utils.ParserWritable;


public class Inverter extends Reducer<ParserWritable, ParserWritable, Text, Text> {
    private static final Logger logger = LogManager.getLogger(Inverter.class);

    private Storage store;
    private Map<Integer, HitList> hitLists;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String storageDir = conf.get("storageDir");
		try {
			store = StorageFactory.getDatabaseInstance(storageDir);
			hitLists = store.createHitBuffer(context.getTaskAttemptID().getTaskID().getId());
		}
		catch (Exception e) {
			throw new RuntimeException("Unable to create BDB store in inverter");
		}
    }
    
    @Override
    protected void reduce(ParserWritable key, Iterable<ParserWritable> values, Context context) throws IOException, InterruptedException {
        hitLists.clear();
        String term = key.getTerm();

        for (ParserWritable value : values) {
            // term = value.getTerm();
            int docId = value.getDocId();
            int pos = value.getPos();

            HitList hitList = hitLists.get(docId);
            if (hitList == null) {
                hitList = new HitList(docId);
            }

            hitList.add(pos);
            hitLists.put(docId, hitList);
        }
        
        int docFreq = hitLists.size();
        logger.debug("Inverter outputting word: {}, df: {}", term, docFreq);
        
        StringBuilder outputStr = new StringBuilder(String.format("%d:", docFreq));
        for (var entry : hitLists.entrySet()) {
            int docId = entry.getKey();
            HitList hitList = entry.getValue();
            int termFreq = hitList.size();
            
            String joinedHits = hitList.getHits().stream().map(String::valueOf).collect(Collectors.joining(","));
            outputStr.append(String.format("%d,%d|%s;", docId, termFreq, joinedHits));
        }
        context.write(new Text(term), new Text(outputStr.toString()));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        store.close();
    }
}
