package weblinkgraph;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Driver class for WebLinkGraph.
 */
public class WebLinkGraphDriver {
	
	public static void main (String[] args) throws Exception {
				
		if (args.length != 2) {
			System.out.println("Syntax: {input path} {output path}");
			System.exit(1);
		}
		
		String input = args[0];
		String output = args[1];
		
		try {
			// Set configuration for map-reduce and S3 access
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(new URI(output), conf);
			conf.set("mapreduce.input.fileinputformat.split.maxsize", "268435456");	
			
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
		
}