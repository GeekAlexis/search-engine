package stormlite.spout;

import stormlite.OutputFieldsDeclarer;
import stormlite.TopologyContext;
import stormlite.routers.IStreamRouter;
import stormlite.tuple.Fields;
import stormlite.tuple.Values;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

// Spout for url queue
public class URLSpout implements IRichSpout{
	
	static Logger log = LogManager.getLogger(URLSpout.class);
	
	String executorId = UUID.randomUUID().toString();
	SpoutOutputCollector collector;
	
	private int maxCount = 1000;
	private static int currCount = 0;
	
	static ConcurrentLinkedQueue<String> urlQueue;

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url"));
	}

	@Override
	public void open(Map<String, String> config, TopologyContext topo, SpoutOutputCollector collector) {
		this.collector = collector;
		URLSpout.urlQueue = new ConcurrentLinkedQueue<>();
//		this.maxCount = Crawler.getMaxCount();
		this.currCount = 0;
		ArrayList<String> seedUrls = readQueueFromFile();
		urlQueue.addAll(seedUrls);
	}

	@Override
	public void close() {
		saveQueueToFile();
		urlQueue.clear();
		
	}

	@Override
	public void nextTuple() {
		if (urlQueue.size() > 0 && currCount < maxCount) {
			String url = urlQueue.poll();
			log.debug(getExecutorId() + " emitting " + url);
			this.collector.emit(new Values<Object>(url));
			currCount += 1;
		}
		Thread.yield();
		
	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}
	
	// Add extracted urls from bolts to queue
	public static void addURL(ArrayList<String> urls) {
		for (String url: urls){
			if (!urlQueue.contains(url)){
				urlQueue.add(url);
			}
		}
	}

	// Add delayed url back to queue
	public static void addURLBack(String url) {
		urlQueue.add(url);
		currCount -= 1;
	}

	// Write remaining urls in the queue to file
	public void saveQueueToFile(){
		System.out.println("saving queue");
		try {
			PrintWriter writer = new PrintWriter(new FileWriter("./urls.txt"));
			for (String url: urlQueue){
//				System.out.println(url);
				writer.println(url);
			}
			writer.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public ArrayList<String> readQueueFromFile(){
		ArrayList<String> urls = new ArrayList<>();

		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader("./urls.txt"));
			String line = reader.readLine();
			while (line != null) {
				if (!line.isBlank()){
					urls.add(line);
				}
				// read next line
				line = reader.readLine();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return urls;

	}
	
	
	

}