package pagerank;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
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
			
			for (int i = 0; i < 10; i++) {
				Path input = null;
				Path output = new Path("rank" + Integer.toString(i + 1));
				
				if (i == 0) {
					input = new Path("graph");
				} else {
					input = new Path("rank" + Integer.toString(i));
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
				
				KeyValueTextInputFormat.addInputPath(job, input);
				
				try {
					fs.delete(output, true);
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(0);
				}
				
				TextOutputFormat.setOutputPath(job, output);
				
				if (!job.waitForCompletion(true)) {
					System.exit(1);
				}
				
				if (i >= 1) {
					try {
						fs.delete(new Path("rank" + Integer.toString(i)), true);
					} catch (Exception e) {
						e.printStackTrace();
						System.exit(0);
					}
				}			
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	
}