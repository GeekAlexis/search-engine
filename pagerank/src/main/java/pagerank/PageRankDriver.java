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
			
			FileSystem fs = FileSystem.get(new URI("s3://rankmid"), conf);
			
			for (int i = 0; i < 10; i++) {
				String input = "";
				String output = "s3://rankmid/iteration" + Integer.toString(i + 1);
				
				if (i == 0) {
					input = "s3://linkgraphmid/mid";
				} else {
					input = "s3://rankmid/iteration" + Integer.toString(i);
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
				
				if (i >= 2) {
					try {
						fs.delete(new Path("s3://ranksmall/iteration" + Integer.toString(i - 1)), true);
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