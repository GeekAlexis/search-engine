package data;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataMapper extends Mapper<LongWritable, Text, Text, Text> {
	
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
	    
	    return c;
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		try {
			String line = value.toString();
			// Split value based on the specified separator
			String[] content = line.split("::\\?\\?<2<spli75tt,");
			int length = content.length;
			
			// Get url and rank
			String url = content[0].replace("'", "''");;
			Double rank = Double.parseDouble(content[length - 1]);
			
			// Connect to database to find corresponding url
			Connection c = connectDB();
			c.setAutoCommit(false);
			
			Statement stmt = c.createStatement();
			String sql = "SELECT id FROM \"Document\" WHERE url = '" + url + "';";

			ResultSet rs = stmt.executeQuery(sql);
			
			// Insert data to the table
			String sql2 = "INSERT INTO \"PageRank\" (doc_id, rank) VALUES (?,?) ON CONFLICT (doc_id) DO NOTHING;";
			PreparedStatement pst = c.prepareStatement(sql2);
						
			if (rs.next()) {
				// Get id
				int id = rs.getInt("id");
	            pst.setInt(1, id);
            	pst.setDouble(2, rank);
            	pst.executeUpdate();      
			}
			
			pst.close();
			rs.close();
			stmt.close();
			c.commit();
			c.setAutoCommit(true);
			c.close();
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}

