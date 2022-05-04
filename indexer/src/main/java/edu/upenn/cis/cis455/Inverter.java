package edu.upenn.cis.cis455;

import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.cis455.storage.HitList;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.cis455.storage.StorageImpl;
import edu.upenn.cis.cis455.utils.ParserWritable;

public class Inverter extends Reducer<ParserWritable, ParserWritable, Text, Text> {
    private static final Logger logger = LogManager.getLogger(Inverter.class);

    private StorageImpl store;
    private Map<Integer, HitList> hitLists;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String storageDir = conf.get("storageDir");

        if (!Files.exists(Paths.get(storageDir))) {
            try {
                Files.createDirectory(Paths.get(storageDir));
            } catch (IOException e) {
                logger.error("An error occured:", e);
			    throw new RuntimeException("Unable to create storage directory");
            }
        }
        
		try {
			store = (StorageImpl)StorageFactory.getDatabaseInstance(storageDir);
			hitLists = store.createHitBuffer(context.getTaskAttemptID().getTaskID().getId());
		}
		catch (Exception e) {
            logger.error("An error occured:", e);
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
        
        StringBuilder outputBuilder = new StringBuilder(String.format("%d:", docFreq));
        for (var entry : hitLists.entrySet()) {
            int docId = entry.getKey();
            HitList hitList = entry.getValue();
            int termFreq = hitList.size();
            
            outputBuilder.append(String.format("\n<%d,%d>", docId, termFreq));
            hitList.getHits().forEach(hit -> outputBuilder.append(String.format("\n%d;", hit)));
        }
        context.write(new Text(term), new Text(outputBuilder.toString()));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        store.close();
    }
}
