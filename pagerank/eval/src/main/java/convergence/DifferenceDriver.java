package convergence;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Driver class for PageRank.
 */
public class DifferenceDriver {
	
	public static void main (String[] args) throws Exception {
		
		try {
			Configuration conf = new Configuration();
			conf.set("fs.s3.awsAccessKeyId", "AKIAZFMQMWQO4MKB2PEH");
			conf.set("fs.s3.awsSecretAccessKey", "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");
			conf.set("textinputformat.record.delimiter", "::spli75tt.\n");
			
			String input1 = "s3://1-20000/iteration9/";
			String input2 = "s3://1-20000/iteration10/";
			String output = "s3://rankdifference/10";
			
			FileSystem fs = FileSystem.get(new URI(output), conf);
			
			
				// Create job
				Job job = Job.getInstance(conf);
				job.setJobName("Difference");
				job.setNumReduceTasks(1);
				
				job.setJarByClass(DifferenceDriver.class);
				job.setReducerClass(DifferenceReducer.class);
				
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(DoubleWritable.class);
				
				MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class, PrevDifferenceMapper.class);
				MultipleInputs.addInputPath(job, new Path(input2), TextInputFormat.class, CurrentDifferenceMapper.class);
				
				// Add input path, delete existing output path, and set output path
				try {
					fs.delete(new Path(output), true);
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(0);
				}
				
				TextOutputFormat.setOutputPath(job, new Path(output));
				
				if (job.waitForCompletion(true)) {
					System.out.println("Difference done");
					System.exit(0);
				} else {
					System.out.println("Difference failed");
					System.exit(1);
				}			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
}