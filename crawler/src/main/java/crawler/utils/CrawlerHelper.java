package crawler.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import javax.net.ssl.HttpsURLConnection;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;

public class CrawlerHelper {
	
	final static Logger logger = LogManager.getLogger(CrawlerHelper.class);
	
	// GET Http
	public static String getHttp(String url, URLInfo info) {
		try {
			URL u = new URL(url);

			HttpURLConnection conn = null;
			if (info.isSecure()){
				conn = (HttpsURLConnection) u.openConnection();
			} else{
				conn = (HttpURLConnection) u.openConnection();
			}

			conn.setRequestMethod("GET");
			conn.addRequestProperty("User-Agent", "cis455crawler");
			
			int responseCode = conn.getResponseCode();
			if (responseCode == HttpURLConnection.HTTP_OK) { // success
				BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
				StringBuffer response = new StringBuffer();
				
				int value = 0;
				while ((value = in.read()) != -1) {
					char c = (char)value;
					response.append(c);
				}
				in.close();
				return response.toString();
			} else {
				System.out.println("GET request not worked");
				return null;
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	// Extract links in html docs
	public static ArrayList<String> traverseHtml(String html, String url) {
		Document doc = Jsoup.parse(html);
		Elements links = doc.select("a[href]");
		ArrayList<String> newLinks = new ArrayList<>();
			
		for (Element link: links) {
			String newLink = link.attr("href");
			
			URLInfo u = new URLInfo(url);
			if (newLink.startsWith("https://") || newLink.startsWith("http://")) {
				newLinks.add(newLink);
			} else if (newLink.startsWith("/")) {
				newLink = (u.isSecure() ? "https://" : "http://") + u.getHostName() + newLink;
			} else {
				if (url.endsWith("html") || url.endsWith("htm") || url.endsWith("xml")){
					if (url.contains("/")) {
						String path = url.substring(0, url.lastIndexOf("/"));
						newLink = path + "/" + newLink;
					} else {
						newLink = (u.isSecure() ? "https://" : "http://") + u.getHostName() + "/" + newLink;
					}
				} else if (url.endsWith("/")) {
					newLink = url + newLink;
				} else {
					newLink = url + "/" + newLink;
				}
			}
			newLinks.add(newLink);
		}
		return newLinks;
			
	}
	
	// Check HEAD request type and size
	public static boolean checkValid(String url, int maxFileSize, HttpURLConnection conn) {
		String type = conn.getContentType();
		Long length = conn.getContentLengthLong();
		String lang = conn.getHeaderField("Content-Language");

		// Check type
		if (type == null){
			return false;
		}
		if (type.contains(";")){
			type = type.split(";")[0];
		}
    	if (!type.startsWith("text/html") && !type.startsWith("text/xml") && !type.startsWith("application/xml") && !type.endsWith("+xml")) {
//			logger.info(url + ": illegal content type");
			System.out.println("type: " + type);
			System.out.println(url + ": illegal content type");
			return false;
		}
    	// Check size
		if (length > ((long) maxFileSize * 1024 * 1024)) {
//			logger.info(url + ": reached max size");
			System.out.println("length: " + length);
			System.out.println(url + ": reached max size");
			return false;
		} else if (length < 0) {
//			logger.info(url + ": illegal size");
			System.out.println(url + ": illegal size");
			return false;
		}
		// Check language
		if (lang != null && !lang.contains("en")){
			System.out.println("not in english");
			return false;
		}

    	
    	return true;
    }

}
