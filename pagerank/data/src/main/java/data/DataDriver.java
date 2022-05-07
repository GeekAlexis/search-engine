package data;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

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
    	
    	if (args.length != 2) {
			System.out.println("Syntax: {input path} {output path}");
			System.exit(1);
		}
    	
    	try {
    		String input = args[0];
    		String output = args[1];
    		
			Connection c = connectDB();
			createTable(c);
			c.close();
		      				
			// Set configuration for map-reduce and S3 access
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(new URI(output), conf);
			conf.set("textinputformat.record.delimiter", "::spli75tt.\n");
			
			for (int i = 0; i < 10; i++) {
				String inputfile = input + "/part-r-0000" + Integer.toString(i);
				
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
    		c.setAutoCommit(false);    		
    		stmt = c.createStatement();
    		String sql = "CREATE TABLE \"PageRank\" "
    				+ "(" 
    				+ "doc_id INT NOT NULL PRIMARY KEY,"
                    + "rank DOUBLE PRECISION NOT NULL DEFAULT 0.15"
                    + ");";
    		
            stmt.executeUpdate("DROP TABLE IF EXISTS \"PageRank\"");
            stmt.executeUpdate(sql);
            c.commit();
            c.setAutoCommit(true);
            stmt.close();
            System.out.println("Created table");
    		
    	} catch (Exception e) {
    		e.printStackTrace();
    		System.out.println("Failed to create table");
    		System.exit(1);
    	}
    }
    
}
