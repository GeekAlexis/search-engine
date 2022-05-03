package pagerank;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * Transfer pagerank output data from S3 to RDS.
 */
public class DataTransfer {  
	
    public static void main(String[] args) {
    	
        try {
        	Connection c = connectDB();
        	createTable(c);
            insertTable(c, "1-20000", "x");
            c.close();
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
	        
	    } catch (Exception e) {
	        e.printStackTrace();
	        System.exit(1);
	    }
	    
	    System.out.println("Opened database successfully");
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
    		stmt = c.createStatement();
    		String sql = "CREATE TABLE \"PageRank\" "
    				+ "(" 
    				+ "id INT PRIMARY KEY,"
                    + "rank DOUBLE PRECISION NOT NULL DEFAULT 0"
                    + ");";
    		
            stmt.executeUpdate("DROP TABLE IF EXISTS \"PageRank\"");
            stmt.executeUpdate(sql);
            stmt.close();
    		
    	} catch (Exception e) {
    		e.printStackTrace();
    		System.exit(1);
    	}
    }

    /**
     * Insert pagerank output into table.
     * @param c connection
     * @param bucket S3 bucket name
     * @param path S3 path
     * @throws Exception
     */
    public static void insertTable(Connection c, String bucket, String path) throws Exception {
    	
    	// Connect to S3.
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
        			.withRegion(Regions.US_EAST_1)
                    .withCredentials(new ProfileCredentialsProvider())
                    .build();
                
        try (S3Object object = s3.getObject(new GetObjectRequest(bucket, path));
             InputStream objectData = object.getObjectContent())
        {
            // Read input file
            BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
            String line = null;
            
            String sql = "INSERT INTO \"PageRank\" (url, rank) VALUES (?,?);";
            PreparedStatement pst = c.prepareStatement(sql);
            
            c.setAutoCommit(false);
            
            int i = 0;

            while ((line = reader.readLine()) != null) {
            	System.out.println(i);
            	
            	
            	// Split into key and value
            	String[] content = line.split("\t");
            	// Get url, which is key
            	String url = content[0].trim().replace("'", "''");
            	// Check if url is stored in Document
            	Statement stmt = c.createStatement();
            	String query = "SELECT id FROM \"Document\" WHERE url = '" + url + "';";
    			ResultSet rs = stmt.executeQuery(query);
    			
    			if (rs.next()) {
	            	if (content.length != 0) { 
	            		// Get rank value
		            	String[] info = content[1].split("\\^\\^\\^");
		            	Double rank = Double.parseDouble(info[info.length - 1]);
		            	int id = rs.getInt("id");
		            	pst.setInt(1, id);
		            	pst.setDouble(2, rank);
		            	pst.addBatch();
	            	}
    			}
            }

            pst.executeBatch();
            c.commit();
            c.setAutoCommit(true);

            pst.close();
            reader.close();
            System.out.println("Insertion finished");
            
        } catch (Exception e) {
        	e.printStackTrace();
        	System.exit(1);
        }
    }
    

    
}
