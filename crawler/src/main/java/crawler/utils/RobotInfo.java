package crawler.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

// Info holder for robots.txt
public class RobotInfo {
	
	private String url;
	private int delay;
	private ArrayList<String> disallowed;
	private long lastCrawled;
	private boolean notFound;
	
	public RobotInfo(String url) {
		this.url = url;
		this.delay = 0;
		this.disallowed = new ArrayList<String>();
		this.lastCrawled = 0;
		this.notFound = false;
	}
	
	// Parse robots.txt 
	public void parseTxt(String txt) {
		BufferedReader reader = new BufferedReader(new StringReader(txt));
		try {
			String line = reader.readLine();
			while (line != null) {
				if (line.equals("User-agent: *") || line.equals("user-agent: *") || line.equals("User-Agent: *")) {
					line = reader.readLine();

					while (line != null){
						if (line.startsWith("Disallow: ")) {
							disallowed.add(line.substring(10));
						} else if (line.startsWith("Crawl-delay: ")){
							delay = Integer.parseInt(line.substring(13));
						} else if (line.startsWith("User-agent:") || line.startsWith("user-agent:") || line.startsWith("User-Agent:")){
							System.out.println("done parsing robots.txt");
							return;
						}
						line = reader.readLine();
					}
				}

				if (line == null){
					System.out.println("done parsing robots.txt");
					return;
				}
				line = reader.readLine();
			}
			System.out.println("done parsing robots.txt");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// Determine if the path is allowed to crawl
	public boolean allowToCrawl(String path) {
		if (notFound) {
			return true;
		}
		if (disallowed.contains("/") || disallowed.contains(path)) {
			return false;
		}
	
		String[] pathList = path.split("/");
		
		for (int i = 0; i < pathList.length; i++) {
			String subStr = joinPath(pathList, i);
			if (disallowed.contains(subStr)) {
				return false;
			}
		}
		return true;
		
	}
	
	// Check if need to delay the crawl
	public boolean needToDelay() {
		if ((System.currentTimeMillis() - lastCrawled) < (delay * 1000)) {
			return true;
		}
		return false;
	}
	
	// Helper function
	public void setLastCrawled() {
		lastCrawled = System.currentTimeMillis();
	}
	
	// Helper function
	public String joinPath(String[] pathList, int idx) {
		String ret = "";
		for (int i = 0; i <= idx; i++) {
			ret = ret + pathList[i] + "/";
		}
		return ret;
	}
	
	// Helper function
	public void notFound() {
		this.notFound = true;
	}
	

}
