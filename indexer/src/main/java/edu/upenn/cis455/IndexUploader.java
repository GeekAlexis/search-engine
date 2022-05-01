package edu.upenn.cis455;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.sql.PreparedStatement;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;


public class IndexUploader {  
    private static final Regions CLIENT_REGION = Regions.US_EAST_1;
    static final String DB_USER = "postgres";
    static final String DB_PASS = "cis555db";

    public static void parseIndexFile(String bucketName, String key) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(CLIENT_REGION)
                    .withCredentials(new ProfileCredentialsProvider())
                    .build();
 
        Pattern indexPattern = Pattern.compile("^(\\p{ASCII}+),(\\d+)|(\\d+),(\\d+):((\\d+,)*\\d+);$");

        try (S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, key));
             InputStream objectData = object.getObjectContent())
        {
            // Read the text input stream one line at a time
            BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
            String line = null;
            while ((line = reader.readLine()) != null) {
                Matcher m = indexPattern.matcher(line);

                if (m.find()) {
                    String term = m.group(1);
                    int df = Integer.parseInt(m.group(2));
                    int docId = Integer.parseInt(m.group(3));
                    int tf = Integer.parseInt(m.group(4));
                    
                    for (String posStr : m.group(5).split(",")) {
                        int position = Integer.parseInt(posStr);
                        
                    }
                }
                else {
                    System.err.println("Failed to parse line in index file");
                }

                // String[] splits = line.split(":");
                // String term = splits[0].split(",")[0];
                // String df = splits[0].split(",")[1];

                // String[] postings = splits[1].split(";");
                // for (String posting : postings) {
                //     splits = posting.split("|");
                //     splits[0].split(",");
                // }
            }
                
        } catch (IOException e) {
            System.err.println("An error occured: " + e);
        }
        
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Syntax: IndexUploader {index bucket} {index key} {database url}");
            System.exit(1);
        }

        // // Get credentials from env variables
        // String user = System.getProperty("DATABASE_USER");
        // String password = System.getProperty("DATABASE_PASS");

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("Failed to open database: " + e);
        }

        try (Connection conn = DriverManager.getConnection(args[2], DB_USER, DB_PASS);
             Statement stmt = conn.createStatement())
        {
            String hitsTable = "CREATE TABLE IF NOT EXISTS Hits (" +
                               "id SERIAL PRIMARY KEY," +
                               "position INTEGER NOT NULL)";

            // String postingsTable = "CREATE TABLE IF NOT EXISTS Postings (" +
            //                        "id SERIAL PRIMARY KEY," +
            //                        "doc_id INTEGER," +
            //                        "tf INTEGER," +
            //                        "hid_offset INTEGER REFERENCES Hits (id))";

            String postingsTable = "CREATE TABLE IF NOT EXISTS Postings (" +
                                   "id SERIAL PRIMARY KEY," +
                                   "doc_id INTEGER REFERENCES Document (id)," +
                                   "tf INTEGER," +
                                   "hid_offset INTEGER REFERENCES Hits (id))";

            String lexiconTable = "CREATE TABLE IF NOT EXISTS Lexicon (" +
                                  "id SERIAL PRIMARY KEY," +
                                  "term TEXT NOT NULL," +
                                  "df INTEGER," +
                                  "pid_offset INTEGER REFERENCES Postings (id))";
            
            stmt.executeUpdate(hitsTable);
            stmt.executeUpdate(postingsTable);
            stmt.executeUpdate(lexiconTable);

            
        } catch (SQLException e) {
            System.err.println("Failed to open database: " + e);
        }


    }
}
