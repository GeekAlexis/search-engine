package weblinkgraph;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Driver class for WebLinkGraph.
 */
public class WebLinkGraphDriver {
	
	public static void main (String[] args) throws Exception {
		
		// Set input and output paths in S3
		String input = "s3://555docbucket/in/";          
		String output = "s3://graphall/graph/";

		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(new URI(output), conf);
			conf.set("mapreduce.input.fileinputformat.split.maxsize", "268435456");	
			conf.set("fs.s3.awsAccessKeyId", "AKIAZFMQMWQO4MKB2PEH");
			conf.set("fs.s3.awsSecretAccessKey", "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");
			
			// Create job
			Job job = Job.getInstance(conf);
			job.setJobName("WebLinkGraph");
			
			job.setJarByClass(WebLinkGraphDriver.class);
			job.setMapperClass(WebLinkGraphMapper.class);
			job.setReducerClass(WebLinkGraphReducer.class);
			
			job.setInputFormatClass(WholeFileInputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			// Add input path, delete any existing output path, and set output path
			WholeFileInputFormat.addInputPath(job, new Path(input));
			
			try {
				fs.delete(new Path(output), true);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			TextOutputFormat.setOutputPath(job, new Path(output));
			
			if (job.waitForCompletion(true)) {
				System.out.println("WebLinkGraph done");
				System.exit(0);
			} else {
				System.out.println("WebLinkGraph failed");
				System.exit(1);
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
	
	
}