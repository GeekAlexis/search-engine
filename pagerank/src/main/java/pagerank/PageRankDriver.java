package pagerank;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Driver class for PageRank.
 */
public class PageRankDriver {
	
	public static void main (String[] args) throws Exception {
		
		try {
			Connection c = connectDB();
			createTable(c);
			c.close();
			
			Configuration conf = new Configuration();
			conf.set("fs.s3.awsAccessKeyId", "AKIAZFMQMWQO4MKB2PEH");
			conf.set("fs.s3.awsSecretAccessKey", "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");
			conf.set("textinputformat.record.delimiter", "::spli75tt.\n");
			
			String input = "s3://graphall/output/";
			String s3Output = "s3://1-20000/";
			
			FileSystem fs = FileSystem.get(new URI(s3Output), conf);
			
			// Run pagerank for each iteration
			for (int i = 0; i < 2; i++) {
				
				if (i != 0) {
					input = s3Output + "iteration" + Integer.toString(i);
				}
				
				String output = s3Output + "iteration" + Integer.toString(i + 1);
				
				// Create job
				Job job = Job.getInstance(conf);
				job.setNumReduceTasks(10);
				job.setJobName("PageRank");
				job.setJarByClass(PageRankDriver.class);
				job.setMapperClass(PageRankMapper.class);
				job.setReducerClass(PageRankReducer.class);
				
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				
				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				
				// Add input path, delete existing output path, and set output path
				TextInputFormat.addInputPath(job, new Path(input));
					
				try {
					fs.delete(new Path(output), true);
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(0);
				}
				
				TextOutputFormat.setOutputPath(job, new Path(output));
				
				if (!job.waitForCompletion(true)) {
					System.exit(1);
				}
				
				// To save storage, delete output from 2 iterations ago
				if (i >= 2) {
					try {
						fs.delete(new Path(s3Output + "iteration" + Integer.toString(i - 1)), true);
					} catch (Exception e) {
						e.printStackTrace();
						System.exit(1);
					}
				}			
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	/**
     * Set up database connection to AWS RDS Postgresql.
     * @return connection
     */
	public static Connection connectDB() throws Exception {
		
		Connection c = null;
		
	    try {
	        Class.forName("org.postgresql.Driver");
	        c = DriverManager
	                .getConnection("jdbc:postgresql://database-1.cnw1rlie1jes.us-east-1.rds.amazonaws.com:5432/postgresdb",
	                        "postgres", "cis555db");
	        System.out.println("Opened database successfully");
   
	    } catch (Exception e) {
	        e.printStackTrace();
	        System.out.println("Failed to open database");
	        System.exit(1);
	    }
	    
	    return c;
	    
	}
	
	/**
	 * Create table in RDS to store pagerank.
	 * @param c connection
	 * @throws Exception
	 */
    public static void createTable(Connection c) throws Exception {
    	
    	Statement stmt = null;
    	
    	try {
    		stmt = c.createStatement();
    		String sql = "CREATE TABLE \"PageRank\" "
    				+ "(" 
    				+ "id SERIAL PRIMARY KEY"
    				+ "doc_id INT PRIMARY KEY,"
                    + "rank DOUBLE PRECISION NOT NULL DEFAULT 0"
                    + ");";
    		
            stmt.executeUpdate("DROP TABLE IF EXISTS \"PageRank\"");
            stmt.executeUpdate(sql);
            c.commit();
            stmt.close();
            System.out.println("Created table");
    		
    	} catch (Exception e) {
    		e.printStackTrace();
    		System.exit(1);
    	}
    }
	
}