package crawler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import storage.StorageFactory;
import storage.StorageInterface;
import stormlite.Config;
import stormlite.LocalCluster;
import stormlite.Topology;
import stormlite.TopologyBuilder;
import stormlite.bolt.ExtractorBolt;
import stormlite.bolt.FetcherBolt;
import stormlite.spout.URLSpout;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Crawler implements CrawlMaster {
    /// TODO: you'll need to flesh all of this out. You'll need to build a thread
    // pool of CrawlerWorkers etc.

	final static Logger logger = LogManager.getLogger(Crawler.class);

    /**
     * Main thread
     */
    public void start() {
//    	taskQueue.add(startUrl);
    }

    /**
     * We've indexed another document
     */
    @Override
    public void incCount() {
//    	currCount += 1;
    }

    /**
     * Workers can poll this to see if they should exit, ie the crawl is done
     */
    @Override
    public boolean isDone() {
//        if (currCount == maxCount || taskQueue.isEmpty()) {
//        	return true;
//        } else {
//        	return false;
//        }
        return false;
    }

    /**
     * Workers should notify when they are processing an URL
     */
    @Override
    public void setWorking(boolean working) {
    }

    /**
     * Workers should call this when they exit, so the master knows when it can shut
     * down
     */
    @Override
    public void notifyThreadExited() {
    }

    private static final String URL_SPOUT= "URL_SPOUT";
    private static final String FETCHER_BOLT = "FETCHER_BOLT";
    private static final String EXTRACTOR_BOLT = "EXTRACTOR_BOLT";

    /**
     * Main program: init database, start crawler, wait for it to notify that it is
     * done, then close.
     * @throws InterruptedException 
     */
    public static void main(String args[]) throws InterruptedException {
    	org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn.cis.cis455", Level.DEBUG);

        System.out.println("Crawler starting");

        StorageFactory.connectToDatabase();
        StorageInterface storage = StorageFactory.getInstance();

        // Set up StormLite
        Config config = new Config();

        URLSpout spout = new URLSpout();
        FetcherBolt fetcherBolt = new FetcherBolt();
        ExtractorBolt extractorBolt = new ExtractorBolt();

        // Create topology
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(URL_SPOUT, spout, 1);
        builder.setBolt(FETCHER_BOLT, fetcherBolt, 3).shuffleGrouping(URL_SPOUT);
        builder.setBolt(EXTRACTOR_BOLT, extractorBolt, 3).shuffleGrouping(FETCHER_BOLT);

        LocalCluster cluster = new LocalCluster();
        Topology topo = builder.createTopology();

        ObjectMapper mapper = new ObjectMapper();
		try {
			String str = mapper.writeValueAsString(topo);

			System.out.println("The StormLite topology is:\n" + str);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		cluster.submitTopology("test", config,
        		builder.createTopology());
        Thread.sleep(800000);
        cluster.killTopology("test");
        cluster.shutdown();


        // final shutdown
        storage.close();

        System.out.println("Done crawling!");
    }

}
