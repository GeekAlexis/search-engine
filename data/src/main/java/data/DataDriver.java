package data;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Transfer pagerank output data from S3 to RDS.
 */
public class DataDriver {  
	
    public static void main(String[] args) {
    	
    	// Set input and output paths in S3
    			String input = "s3://pagerankresult/iteration100/"; 
    			String output = "s3://1-20000/result/";
    			
    			try {    
    				// Set configuration for map-reduce and S3 access
    				Configuration conf = new Configuration();
    				FileSystem fs = FileSystem.get(new URI(output), conf);
    				conf.set("fs.s3.awsAccessKeyId", "AKIAZFMQMWQO4MKB2PEH");
    				conf.set("fs.s3.awsSecretAccessKey", "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");
    				conf.set("textinputformat.record.delimiter", "::spli75tt.\n");
    				
    				for (int i = 0; i < 7; i++) {
    					String inputfile = input + "part-r-0000" + Integer.toString(i);
    					
	    				// Create job
	    				Job job = Job.getInstance(conf);
	    				job.setNumReduceTasks(1);
	    				job.setJobName("Data");
	    				
	    				job.setJarByClass(DataDriver.class);
	    				job.setMapperClass(DataMapper.class);
	    				job.setReducerClass(DataReducer.class);
	    				
	    				job.setOutputKeyClass(Text.class);
	    				job.setOutputValueClass(Text.class);
	    				
	    				job.setInputFormatClass(TextInputFormat.class);
	    				job.setOutputFormatClass(TextOutputFormat.class);
	    				
	    				// Add input path, delete any existing output path, and set output path
	    				TextInputFormat.addInputPath(job, new Path(inputfile));
	    				
	    				try {
	    					fs.delete(new Path(output), true);
	    				} catch (Exception e) {
	    					e.printStackTrace();
	    				}
	    				
	    				TextOutputFormat.setOutputPath(job, new Path(output + Integer.toString(i)));
	    				
	    				if (!job.waitForCompletion(true)) {
	    					System.exit(1);
	    				} 
    				}
    			
    				System.out.println("Data transfer done");
					System.exit(0);	
    			
    			} catch (Exception e) {
    				e.printStackTrace();
    				System.exit(1);
    			}       
    }    
}
