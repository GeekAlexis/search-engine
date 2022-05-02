package edu.upenn.cis455;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
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


public class IndexUploader {  
    private static final Regions CLIENT_REGION = Regions.US_EAST_1;
    private static final String DB_USER = "postgres";
    private static final String DB_PASS = "cis555db";

    public static void createIndexTables(Connection conn) throws SQLException {
        String hitTable = "CREATE TABLE IF NOT EXISTS \"Hit\" (" +
                          "id SERIAL PRIMARY KEY," +
                          "position INTEGER NOT NULL)";

        // String postingTable = "CREATE TABLE IF NOT EXISTS \"Posting\" (" +
        //                        "id SERIAL PRIMARY KEY," +
        //                        "doc_id INTEGER," +
        //                        "tf INTEGER," +
        //                        "hit_id_offset INTEGER REFERENCES Hits (id))";

        String postingTable = "CREATE TABLE IF NOT EXISTS \"Posting\" (" +
                              "id SERIAL PRIMARY KEY," +
                              "doc_id INTEGER REFERENCES \"Document\" (id)," +
                              "tf INTEGER," +
                              "hit_id_offset INTEGER REFERENCES \"Hit\" (id))";

        String lexiconTable = "CREATE TABLE IF NOT EXISTS \"Lexicon\" (" +
                              "id SERIAL PRIMARY KEY," +
                              "term TEXT NOT NULL UNIQUE," +
                              "df INTEGER," +
                              "posting_id_offset INTEGER REFERENCES \"Posting\" (id))";

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(hitTable);
            stmt.executeUpdate(postingTable);
            stmt.executeUpdate(lexiconTable);
        }
    }

    public static void uploadIndexFile(Connection conn, String bucketName, String key) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(CLIENT_REGION)
                    .withCredentials(new ProfileCredentialsProvider())
                    .build();
 
        Pattern termPattern = Pattern.compile("^(\\p{ASCII}+),(\\d+):$");
        Pattern postingsPattern = Pattern.compile("^<(\\d+),(\\d+):((\\d+,)*\\d+)>$");

        String hitInsert = "INSERT INTO \"Hit\" (position) " +
                           "VALUES (?) RETURNING id";
        String postingInsert = "INSERT INTO \"Posting\" (doc_id, tf, hit_id_offset) " +
                               "VALUES (?, ?, ?) RETURNING id";
        String lexiconInsert = "INSERT INTO \"Lexicon\" (term, df, posting_id_offset) " +
                               "VALUES (?, ?, ?)";

        try (S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, key));
             InputStream objectData = object.getObjectContent())
        {
            List<Integer> posting_ids = new ArrayList<>();
            List<Integer> hit_ids = new ArrayList<>();
        
            String term = null;
            int df = -1;
        
            // Read the text input stream one line at a time
            BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
            String line = null;    

            while ((line = reader.readLine()) != null) {
                Matcher termMatcher = termPattern.matcher(line);
                Matcher postingsMatcher = postingsPattern.matcher(line);

                if (termMatcher.find()) {
                    // Upload the last term
                    if (!posting_ids.isEmpty()) {
                        System.out.println("Inserted Posting IDs: " + posting_ids);
                        try (PreparedStatement pstmtHits = conn.prepareStatement(lexiconInsert)) {
                            pstmtHits.setString(1, term);
                            pstmtHits.setInt(2, df);
                            pstmtHits.setInt(3, posting_ids.get(0));
    
                            pstmtHits.executeUpdate();
                        } catch (SQLException e) {
                            System.err.println("An error occured when uploading to Lexicon: " + e);
                        }
                    }
                    posting_ids.clear();

                    term = termMatcher.group(1);
                    df = Integer.parseInt(termMatcher.group(2));
                }
                else if (postingsMatcher.find()) {
                    int docId = Integer.parseInt(postingsMatcher.group(1));
                    int tf = Integer.parseInt(postingsMatcher.group(2));
                    hit_ids.clear();

                    // Upload hits for each doc ID
                    try (PreparedStatement pstmtHits = conn.prepareStatement(hitInsert)) {
                        for (String posStr : postingsMatcher.group(3).split(",")) {
                            int position = Integer.parseInt(posStr);
                            pstmtHits.setInt(1, position);

                            ResultSet rs = pstmtHits.executeQuery();
                            rs.next();
                            hit_ids.add(rs.getInt(1));
                        }
                    } catch (SQLException e) {
                        System.err.println("An error occured when uploading to Hits: " + e);
                    }
                    System.out.println("Inserted Hit IDs: " + hit_ids);

                    // Now we upload postings for each term
                    try (PreparedStatement pstmtHits = conn.prepareStatement(postingInsert)) {
                        pstmtHits.setInt(1, docId);
                        pstmtHits.setInt(2, tf);
                        pstmtHits.setInt(3, hit_ids.get(0));

                        ResultSet rs = pstmtHits.executeQuery();
                        rs.next();
                        posting_ids.add(rs.getInt(1));
                    } catch (SQLException e) {
                        System.err.println("An error occured when uploading to Postings: " + e);
                    }
                }
                else {
                    System.err.println("Failed to parse line in index file");
                }
            }

            // Upload the last term
            if (!posting_ids.isEmpty()) {
                System.out.println("Inserted Posting IDs: " + posting_ids);
                try (PreparedStatement pstmtHits = conn.prepareStatement(lexiconInsert)) {
                    pstmtHits.setString(1, term);
                    pstmtHits.setInt(2, df);
                    pstmtHits.setInt(3, posting_ids.get(0));

                    pstmtHits.executeUpdate();
                } catch (SQLException e) {
                    System.err.println("An error occured when uploading to Lexicon: " + e);
                }
            }
                
        } catch (IOException e) {
            System.err.println("An error occured: " + e);
        }
        
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Syntax: IndexUploader {bucket} {key} {database url}");
            System.exit(1);
        }

        // // Get credentials from env variables
        // String user = System.getProperty("DATABASE_USER");
        // String pass = System.getProperty("DATABASE_PASS");

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("Failed to find database driver: " + e);
        }

        try (Connection conn = DriverManager.getConnection(args[2], DB_USER, DB_PASS)) {
            createIndexTables(conn);
            uploadIndexFile(conn, args[0], args[1]);
        } catch (SQLException e) {
            System.err.println("An error occured: " + e);
        }
    }
}
