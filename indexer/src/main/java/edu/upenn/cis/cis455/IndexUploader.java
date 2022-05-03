package edu.upenn.cis.cis455;

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


public class IndexUploader {  
    private static final Regions CLIENT_REGION = Regions.US_EAST_1;
    private static final String DB_USER = "postgres";
    private static final String DB_PASS = "cis555db";

    public static void createInvertedIndexTables(Connection conn) throws SQLException {
        String hitTable = "CREATE TABLE \"Hit\" (" +
                          "id SERIAL PRIMARY KEY," +
                          "position INTEGER NOT NULL)";

        // String postingTable = "CREATE TABLE \"Posting\" (" +
        //                        "id SERIAL PRIMARY KEY," +
        //                        "doc_id INTEGER," +
        //                        "tf INTEGER," +
        //                        "hit_id_offset INTEGER REFERENCES Hits (id))";

        String postingTable = "CREATE TABLE \"Posting\" (" +
                              "id SERIAL PRIMARY KEY," +
                              "doc_id INTEGER REFERENCES \"Document\" (id)," +
                              "tf INTEGER," +
                              "hit_id_offset INTEGER REFERENCES \"Hit\" (id))";

        String lexiconTable = "CREATE TABLE \"Lexicon\" (" +
                              "id SERIAL PRIMARY KEY," +
                              "term TEXT NOT NULL UNIQUE," +
                              "df INTEGER," +
                              "posting_id_offset INTEGER REFERENCES \"Posting\" (id))";

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP TABLE IF EXISTS \"Lexicon\"");
            stmt.executeUpdate("DROP TABLE IF EXISTS \"Posting\"");
            stmt.executeUpdate("DROP TABLE IF EXISTS \"Hit\"");
            
            stmt.executeUpdate(hitTable);
            stmt.executeUpdate(postingTable);
            stmt.executeUpdate(lexiconTable);
        }
    }

    public static void createForwardIndexTable(Connection conn) throws SQLException {
        String forwardIndexTable = "CREATE TABLE \"ForwardIndex\" AS " +
                                   "SELECT doc_id, SUM(tf) AS length FROM \"Posting\" GROUP BY doc_id";

        String addPrimaryKey = "ALTER TABLE \"ForwardIndex\" ADD PRIMARY KEY (doc_id)";

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP TABLE IF EXISTS \"ForwardIndex\"");

            stmt.executeUpdate(forwardIndexTable);
            stmt.executeUpdate(addPrimaryKey);
        }
    }

    public static void uploadIndexFile(Connection conn, String bucketName, String key) throws Exception {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(CLIENT_REGION)
                    .withCredentials(new ProfileCredentialsProvider())
                    .build();
 
        Pattern termPattern = Pattern.compile("^(.+),(\\d+):$");
        Pattern postingsPattern = Pattern.compile("^<(\\d+),(\\d+):((\\d+,)*\\d+)>$");

        String hitInsert = "INSERT INTO \"Hit\" (position) " +
                           "VALUES (?)";
        String postingInsert = "INSERT INTO \"Posting\" (doc_id, tf, hit_id_offset) " +
                               "VALUES (?, ?, ?)";
        String lexiconInsert = "INSERT INTO \"Lexicon\" (term, df, posting_id_offset) " +
                               "VALUES (?, ?, ?)";

        try (S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, key));
             InputStream objectData = object.getObjectContent())
        {
            int fileSize = (int)object.getObjectMetadata().getContentLength();
            int totalRead = 0;

            List<Integer> posting_ids = new ArrayList<>();
            List<Integer> hit_ids = new ArrayList<>();
        
            String term = null;
            int df = -1;

            PreparedStatement pstmtTerm = conn.prepareStatement(lexiconInsert);
            PreparedStatement pstmtHit = conn.prepareStatement(hitInsert, Statement.RETURN_GENERATED_KEYS);
            PreparedStatement pstmtPosting = conn.prepareStatement(postingInsert, Statement.RETURN_GENERATED_KEYS);
        
            // Read the text input stream one line at a time
            BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
            String line = null;

            conn.setAutoCommit(false);

            while ((line = reader.readLine()) != null) {
                Matcher termMatcher = termPattern.matcher(line);
                Matcher postingsMatcher = postingsPattern.matcher(line);

                if (termMatcher.find()) {
                    // Add the last term
                    if (term != null) {
                        try {
                            pstmtPosting.executeBatch();

                            ResultSet rs = pstmtPosting.getGeneratedKeys();
                            posting_ids.clear();
                            while (rs.next()) {
                                posting_ids.add(rs.getInt(1));
                            }
                            // System.out.println("Inserted Posting IDs: " + posting_ids);

                            pstmtTerm.setString(1, term);
                            pstmtTerm.setInt(2, df);
                            pstmtTerm.setInt(3, Collections.min(posting_ids));
                            
                            pstmtTerm.addBatch();
                        } catch (SQLException e) {
                            System.err.println("An error occured when uploading to Posting and Lexicon: " + e);
                        }
                    }
                    
                    term = termMatcher.group(1);
                    df = Integer.parseInt(termMatcher.group(2));
                }
                else if (postingsMatcher.find()) {
                    int docId = Integer.parseInt(postingsMatcher.group(1));
                    int tf = Integer.parseInt(postingsMatcher.group(2));
                    
                    try {
                        // Add hits for each doc ID
                        for (String posStr : postingsMatcher.group(3).split(",")) {
                            int position = Integer.parseInt(posStr);
                            
                            pstmtHit.setInt(1, position);
                            pstmtHit.addBatch();
                        }

                        pstmtHit.executeBatch();
                        
                        ResultSet rs = pstmtHit.getGeneratedKeys();
                        hit_ids.clear();
                        while (rs.next()) {
                            hit_ids.add(rs.getInt(1));
                        }
                        // System.out.println("Inserted Hit IDs: " + hit_ids);

                    } catch (SQLException e) {
                        System.err.println("An error occured when uploading to Hits: " + e);
                    }                    

                    // Now we add postings for each term
                    try {
                        pstmtPosting.setInt(1, docId);
                        pstmtPosting.setInt(2, tf);
                        pstmtPosting.setInt(3, Collections.min(hit_ids));
                        pstmtPosting.addBatch();

                        // ResultSet rs = pstmtPosting.getGeneratedKeys();
                        // rs.next();
                        // posting_ids.add(rs.getInt(1));
                    } catch (SQLException e) {
                        System.err.println("An error occured when adding Posting batch: " + e);
                    }
                }
                else {
                    System.err.println("Failed to parse line in index file: " + line);
                }

                if (totalRead % 1024 == 0) {
                    conn.commit();
                    System.out.println("Progress ===> " + String.format("%.3f%%", (double)totalRead / fileSize * 100));
                }
                totalRead += line.length() + 1;
            }

            // Add the last term
            if (term != null) {    
                try {
                    pstmtPosting.executeBatch();

                    ResultSet rs = pstmtPosting.getGeneratedKeys();
                    posting_ids.clear();
                    while (rs.next()) {
                        posting_ids.add(rs.getInt(1));
                    }
                    // System.out.println("Inserted Posting IDs: " + posting_ids);

                    pstmtTerm.setString(1, term);
                    pstmtTerm.setInt(2, df);
                    pstmtTerm.setInt(3, Collections.min(posting_ids));
                    
                    pstmtTerm.addBatch();
                } catch (SQLException e) {
                    System.err.println("An error occured when uploading to Posting and Lexicon: " + e);
                }
            }
            System.out.println("Progress ===> " + String.format("%.3f%%", (double)totalRead / fileSize * 100));

            // Upload all terms
            pstmtTerm.executeBatch();
            conn.commit();
            conn.setAutoCommit(true);

            pstmtTerm.close();
            pstmtPosting.close();
            pstmtHit.close();
            reader.close();
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
            System.err.println("Creating inverted index tables...");
            createInvertedIndexTables(conn);
            
            System.err.println("Uploading index file...");
            uploadIndexFile(conn, args[0], args[1]);

            System.err.println("Creating forward index table...");
            createForwardIndexTable(conn);

            System.err.println("Done!");
        } catch (Exception e) {
            System.err.println("An error occured: " + e);
        }
        
    }
}
