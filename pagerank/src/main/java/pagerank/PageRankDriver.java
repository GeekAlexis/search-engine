package pagerank;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRankDriver {
	public static void main (String[] args) throws Exception {
		
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			
			//FileSystem fs = FileSystem.get(new URI("s3://ranksmall"), conf);
			
			for (int i = 0; i < 1; i++) {
				String input = "";
				String output = "iterate" + Integer.toString(i + 1);
				// String output = "s3://ranksmall/iteration" + Integer.toString(i + 1);
				
				if (i == 0) {
					input = "g";
					//input = "s3://linkgraphsmall/small";
				} else {
					input = "iterate" + Integer.toString(i);
					//input = "s3://ranksmall/iteration" + Integer.toString(i);
				}
				
				Job job = Job.getInstance(conf);
				job.setJobName("PageRank");
				job.setJarByClass(PageRankDriver.class);
				job.setMapperClass(PageRankMapper.class);
				job.setReducerClass(PageRankReducer.class);
				
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				
				job.setInputFormatClass(KeyValueTextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				
				KeyValueTextInputFormat.addInputPath(job, new Path(input));
				
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
				
//				if (i >= 2) {
//					try {
//						fs.delete(new Path("iterate" + Integer.toString(i - 1)), true);
//						//fs.delete(new Path("s3://ranksmall/iteration" + Integer.toString(i)), true);
//					} catch (Exception e) {
//						e.printStackTrace();
//						System.exit(1);
//					}
//				}			
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
}