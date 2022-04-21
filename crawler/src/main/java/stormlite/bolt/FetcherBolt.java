package stormlite.bolt;

import crawler.utils.CrawlerHelper;
import crawler.utils.RobotInfo;
import crawler.utils.URLInfo;
import storage.StorageFactory;
import storage.StorageInterface;
import stormlite.OutputFieldsDeclarer;
import stormlite.TopologyContext;
import stormlite.routers.IStreamRouter;
import stormlite.tuple.Fields;
import stormlite.tuple.Tuple;
import stormlite.tuple.Values;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.HttpsURLConnection;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

// Fetch document from a url and send it to extractor
public class FetcherBolt implements IRichBolt{
	static Logger logger = LogManager.getLogger(FetcherBolt.class);
	
	String executorId = UUID.randomUUID().toString();
	Fields schema = new Fields("url", "document");
	
	private OutputCollector collector;
	
	private HashMap<String, RobotInfo> robotMap;
	private StorageInterface db;
	private int maxFileSize;
	

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
	public void execute(Tuple input) {
		String url = input.getStringByField("url");
		
		URLInfo info = new URLInfo(url);
		
		// Check robots.txt
		if (robotMap.containsKey(info.getHostName())) {
			if (!robotMap.get(info.getHostName()).allowToCrawl(info.getFilePath())) {
				logger.info(url + ": robot disallow");
				return;
			}
		} else {
			String robotUrl = (info.isSecure() ? "https://" : "http://") + info.getHostName() + "/robots.txt";
			System.out.println(robotUrl);
			String robotTxt = CrawlerHelper.getHttp(robotUrl, info);

			RobotInfo rInfo = new RobotInfo(info.getHostName());
			if (robotTxt == null) {
				rInfo.notFound();
			} else {
				rInfo.parseTxt(robotTxt);
			}
			robotMap.put(info.getHostName(), rInfo);
			
			if (!rInfo.allowToCrawl(info.getFilePath())) {
				logger.info(url + ": robot disallow");
				System.out.println(url + ": robot disallow");
				return;
			}
		}
		
		
		// Check if need to delay 
		if (robotMap.get(info.getHostName()).needToDelay()) {
			collector.emit(new Values<Object>(url, null));
			return;
		}
		
		// Send HEAD request first
		String html = null;
		try {
			URL u = new URL(url);

			HttpURLConnection conn = null;
			if (info.isSecure()){
				conn = (HttpsURLConnection) u.openConnection();
			} else{
				conn = (HttpURLConnection) u.openConnection();
			}

			conn.setRequestMethod("HEAD");
			conn.addRequestProperty("User-Agent", "cis455crawler");

			int responseCode = conn.getResponseCode();
			if (responseCode == HttpURLConnection.HTTP_OK) { // success
				String type = conn.getContentType();
				Long length = conn.getContentLengthLong();
				if (!CrawlerHelper.checkTypeSize(type, length, url, maxFileSize)) {
					System.out.println(url + ": not valid type");
					return;
				}
				Long lastModified = conn.getLastModified();
				if (db.getDocument(url) != null) {
					// don't crawl but process
					if (lastModified <= db.getCrawledTime(url)) {
						logger.info(url + ": not modified");
						System.out.println(url + ": not modified");
						html = db.getDocument(url);
					}
				}
			} else if ((responseCode == HttpURLConnection.HTTP_MOVED_PERM)|| (responseCode == HttpURLConnection.HTTP_MOVED_TEMP)) {
				String newUrl = conn.getHeaderField("Location");
				if (newUrl != null) {
					collector.emit(new Values<Object>(newUrl, null));
				}
				return;

			} else {
				System.out.println("HEAD request not worked");
				return;
			}

		} catch (MalformedURLException e) {
			e.printStackTrace();
			return;
		} catch (ProtocolException e){
			e.printStackTrace();
			return;
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		// Send GET request
		if (html == null) {
			html = CrawlerHelper.getHttp(url, info);
		}
		robotMap.get(info.getHostName()).setLastCrawled();

		// Check if have seen the content
		if (db.checkSeen(html)) {
			logger.info(url + ": content seen");
			System.out.println(url + ": content seen");
			return;
		}
		collector.emit(new Values<Object>(url, html));
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.db = StorageFactory.getInstance();
		this.maxFileSize = 1; // Set max file size to 1
		this.robotMap =  new HashMap<String, RobotInfo>();
		
	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
		
	}

	@Override
	public Fields getSchema() {
		return schema;
	}
	

}
