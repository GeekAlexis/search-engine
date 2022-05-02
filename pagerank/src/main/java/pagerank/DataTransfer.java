package pagerank;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;


public class DataTransfer {      
    public static void main(String[] args) {
    	
        try {
        	Connection c = connectDB();
        	createTable(c);
            insertTable(c, "ranksmall", "iteration1/part-r-00000");
            c.close();
           
        } catch (Exception e) {
        	e.printStackTrace();
            System.err.println("An error occured: " + e);
        }
        
    }
    
	public static Connection connectDB() throws Exception {
		// Set up database connection to AWS RDS Postgresql
		Connection c = null;
		
	    try {
	        Class.forName("org.postgresql.Driver");
	        c = DriverManager
	                .getConnection("jdbc:postgresql://database-1.cnw1rlie1jes.us-east-1.rds.amazonaws.com:5432/postgresdb",
	                        "postgres", "cis555db");
	        
	    } catch (Exception e) {
	        e.printStackTrace();
	        System.exit(1);
	    }
	    
	    System.out.println("Opened database successfully");
	    return c;
}

    public static void createTable(Connection c) throws Exception {
    	Statement stmt = null;
    	
    	try {
    		stmt = c.createStatement();
    		String sql = "CREATE TABLE \"PageRank\" "
    				+ "(" 
    				+ "id SERIAL PRIMARY KEY,"
                    + "url TEXT NOT NULL,"
                    + "rank DOUBLE PRECISION NOT NULL DEFAULT 0"
                    + ")";
    		
            stmt.executeUpdate("DROP TABLE IF EXISTS \"PageRank\"");
            stmt.executeUpdate(sql);
            stmt.close();
    		
    	} catch (Exception e) {
    		e.printStackTrace();
    		System.exit(1);
    	}
    }

    public static void insertTable(Connection c, String bucket, String key) throws Exception {
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
        			.withRegion(Regions.US_EAST_1)
                    .withCredentials(new ProfileCredentialsProvider())
                    .build();
        
        String sql = "INSERT INTO \"PageRank\" (url, rank) VALUES (?,?);";
        
        try (S3Object object = s3.getObject(new GetObjectRequest(bucket, key));
             InputStream objectData = object.getObjectContent())
        {
            // Read the text input stream one line at a time
            BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
            String line = null;

            PreparedStatement pst = c.prepareStatement(sql);
            c.setAutoCommit(false);

            while ((line = reader.readLine()) != null) {
            	String[] content = line.split("\t");
            	String url = content[0];
            	String[] info = content[1].split("\\^\\^\\^");
            	Double rank = Double.parseDouble(info[info.length - 1]);
            	pst.setString(1, url);
            	pst.setDouble(2, rank);
            	pst.addBatch();
            }
			
            pst.executeBatch();
            c.commit();
            c.setAutoCommit(true);

            pst.close();
            reader.close();
        } catch (Exception e) {
        	e.printStackTrace();
        	System.exit(1);
        }
    }
    
    

    
}
