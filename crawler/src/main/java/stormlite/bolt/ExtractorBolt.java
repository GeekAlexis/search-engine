package stormlite.bolt;

import crawler.utils.CrawlerHelper;
import storage.StorageFactory;
import storage.StorageInterface;
import stormlite.OutputFieldsDeclarer;
import stormlite.TopologyContext;
import stormlite.routers.StreamRouter;
import stormlite.spout.URLSpout;
import stormlite.tuple.Fields;
import stormlite.tuple.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

// Extract new links in a document and store to database
public class ExtractorBolt implements IRichBolt{
	
	static Logger logger = LogManager.getLogger(ExtractorBolt.class);
	
	String executorId = UUID.randomUUID().toString();
	Fields schema = new Fields();
	
	private StorageInterface db;

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(schema);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
	}

	@Override
	public boolean execute(Tuple input) {
		String url = input.getStringByField("url");
		String doc = input.getStringByField("document");
		
		// Delay
		if (doc == "") {
			URLSpout.addURLBack(url);
			return true;
		}
		
		// Store documents
		db.addDocument(url, doc);
		logger.info(url + ": downloading");
		
		// Find new links
		ArrayList<String> newLinks = CrawlerHelper.traverseHtml(doc, url);
		URLSpout.addURL(newLinks);

		return true;
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		this.db = StorageFactory.getInstance();
	}

	@Override
	public void setRouter(StreamRouter router) {
		// DO nothing
	}

	@Override
	public Fields getSchema() {
		return schema;
	}
	

}
