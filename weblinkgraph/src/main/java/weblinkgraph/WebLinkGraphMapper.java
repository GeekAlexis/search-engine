package weblinkgraph;

import org.apache.hadoop.mapreduce.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.apache.hadoop.io.*;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Mapper class for WebLinkGraph.
 */
public class WebLinkGraphMapper extends Mapper<IntWritable, Text, Text, Text> {
	
	@Override
	protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			// Get file name, which is id
			int id = key.get();
			String content = value.toString();
			
			// Connect to database to find corresponding url
			Connection c = WebLinkGraphDriver.connectDB();
			Statement stmt = c.createStatement();
			String sql = "SELECT DISTINCT url FROM \"Document\" WHERE id = " + id + ";";
			ResultSet rs = stmt.executeQuery(sql);
			
			if (rs.next()) {
				// Get url
                String url = rs.getString("url").trim();
				
				// Extract hyperlinks
				Document doc = Jsoup.parse(content);
				Elements links = doc.select("a");
	            
				for (int i = 0; i < links.size(); i++) {
					Element link = links.get(i);
	              	String linkHref = link.attr("href");
	              	 
	              	if (!linkHref.isEmpty()) {
	              		// special cases: in the same page
	              		if (linkHref.charAt(0) == '#') {
	              			linkHref = url + '/' + linkHref;
	                    } else {
	                    	if (linkHref.length() >= 2) {
	                    		// special cases: relative path
	                    		if (linkHref.charAt(0) == '/' && linkHref.charAt(1) != '/') {
	                    			linkHref = url + linkHref;
	                    		}
	                    		
	                    		// special cases
	                    		if (linkHref.charAt(0) == '/' && linkHref.charAt(1) == '/') {
	                    			linkHref = "https:" + linkHref;
	                    		}
	                    	}
	                    	
	                    	if (linkHref.length() >= 5) {
	                    		// special cases
	                    		if (linkHref.startsWith("&url=")) {
	                    			linkHref = linkHref.substring(5);
	                    		}
	                    	}
	                    }
		                    
	              		// remove trailing /
	                    if (!linkHref.isEmpty() && linkHref.charAt(linkHref.length() - 1) == '/') {
	                    	linkHref = linkHref.substring(0, linkHref.length() - 1);
	                    }
	                    
	                    linkHref = linkHref.trim();
		                
	                    // discard unneeded links telephone, email, and others
	                    if (!linkHref.isEmpty() && linkHref.startsWith("https://")) {
	                    	context.write(new Text(url), new Text(linkHref));
	                    	context.write(new Text(linkHref), new Text(""));
	                    }
	              	}
				}		
			}
			
			rs.close();
			stmt.close();	
			c.close();
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
}
