package weblinkgraph;

import org.apache.hadoop.mapreduce.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.apache.hadoop.io.*;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Mapper class for WebLinkGraph.
 */
public class WebLinkGraphMapper extends Mapper<IntWritable, Text, Text, Text> {
	
	/**
	 * Set up database connection to AWS RDS Postgresql.
	 * @return connection
	 */
	public static Connection connectDB() {
		
		Connection c = null;
		
        try {
            c = DriverManager
                    .getConnection("jdbc:postgresql://database-1.cnw1rlie1jes.us-east-1.rds.amazonaws.com:5432/postgresdb",
                            "postgres", "cis555db");
            
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        
        return c;
	}
	
	@Override
	protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			// Get file name, which is id
			int id = key.get();
			String content = value.toString();
			String identifier = "::??<2<spli75tt,";
			
			// Connect to database to find corresponding url
			Connection c = connectDB();
			Statement stmt = c.createStatement();
			String sql = "SELECT DISTINCT url FROM \"Document\" WHERE id = " + id + ";";
			ResultSet rs = stmt.executeQuery(sql);
			
			if (rs.next()) {
				// Get url
				String url = rs.getString("url");
				
				// Extract hyperlinks
				Document doc = Jsoup.parse(content, url);
				Elements links = doc.select("a[href]");
				
				context.write(new Text(url), new Text(identifier));
				
				for (Element link:links) {
					// Convert them to absolute url
	              	String linkHref = link.attr("abs:href").trim();
	              		              	
                    if (!linkHref.isEmpty()) {	
                    	// key: url, value: its outlink 
                    	context.write(new Text(url), new Text(linkHref));
                    	// key: its outlink, value: empty
                    	context.write(new Text(linkHref), new Text(""));
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
