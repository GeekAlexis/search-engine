package pagerank;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

import java.net.URI;
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
		
		if (args.length != 3) {
			System.out.println("Syntax: {input path} {output path} {number of iteration}");
			System.exit(1);
		}
		
		try {	
			String input = args[0];
			String s3Output = args[1];
			int number = Integer.parseInt(args[2]);
			
			Configuration conf = new Configuration();
			conf.set("textinputformat.record.delimiter", "::spli75tt.\n");			
			FileSystem fs = FileSystem.get(new URI(s3Output), conf);
			
			// Run pagerank for each iteration
			for (int i = 0; i < number; i++) {
				
				if (i != 0) {
					input = s3Output + "/iteration" + Integer.toString(i);
				}
				
				String output = s3Output + "/iteration" + Integer.toString(i + 1);
				
				// Create job
				Job job = Job.getInstance(conf);
				job.setNumReduceTasks(20);
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
						fs.delete(new Path(s3Output + "/iteration" + Integer.toString(i - 1)), true);
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
	
}