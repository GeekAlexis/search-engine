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
//				System.out.println(line);
				if (line.equals("User-agent: *") || line.equals("user-agent: *") || line.equals("User-Agent: *")) {
					line = reader.readLine();

					while (line != null){
//						System.out.println(line);
						if (line.startsWith("Disallow: ")) {
							disallowed.add(line.substring(10));
						} else if (line.startsWith("Crawl-delay: ")){
							delay = Integer.parseInt(line.substring(13));
//							delay = 0;
						} else if (line.isBlank()){
							return;
						}
						line = reader.readLine();
					}
//					if (line == null){
//						break;
//					}
				} 
//				if (line.equals("User-agent: cis455crawler") || line.equals("user-agent: cis455crawler")
//						|| line.equals("User-Agent: cis455crawler")) {
//					disallowed.clear();
//					delay = 0;
//					line = reader.readLine();
//					if(line == null){
//						break;
//					}
//					while (line != null && !line.isBlank()){
//						if (line.startsWith("Disallow: ")) {
//							disallowed.add(line.substring(10));
//						}
//						if (line.startsWith("Crawl-delay: ")){
//							delay = Integer.parseInt(line.substring(13));
////							delay = 0;
//						}
//						line = reader.readLine();
//					}
//					break;
//				}
				
				line = reader.readLine();
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("done parsing robots.txt");
	}
	
	// Determine if the path is allowed to crawl
	public boolean allowToCrawl(String path) {
//		System.out.println(path);
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
