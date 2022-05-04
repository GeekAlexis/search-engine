package edu.upenn.cis.cis455;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class IndexUpload {
    public static void createInvertedIndexTables(Connection conn) throws SQLException {
        String hitTable = "CREATE TABLE \"Hit\" (" +
                          "id INTEGER PRIMARY KEY," +
                          "position INTEGER NOT NULL)";

        String postingTable = "CREATE TABLE \"Posting\" (" +
                              "id INTEGER PRIMARY KEY," +
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

    public static void uploadIndexFile(Connection conn, String indexFile) throws Exception { 
        Pattern termPattern = Pattern.compile("^(.+),(\\d+):$");
        Pattern postingPattern = Pattern.compile("^<(\\d+),(\\d+)>$");
        Pattern hitPattern = Pattern.compile("^(\\d+);$");

        String hitInsert = "INSERT INTO \"Hit\" (id, position) " +
                           "VALUES (?, ?)";
        String postingInsert = "INSERT INTO \"Posting\" (id, doc_id, tf, hit_id_offset) " +
                               "VALUES (?, ?, ?, ?)";
        String lexiconInsert = "INSERT INTO \"Lexicon\" (term, df, posting_id_offset) " +
                               "VALUES (?, ?, ?)";

        PreparedStatement pstmtTerm = conn.prepareStatement(lexiconInsert);
        PreparedStatement pstmtHit = conn.prepareStatement(hitInsert);
        PreparedStatement pstmtPosting = conn.prepareStatement(postingInsert);

        long fileSize = new File(indexFile).length();
        System.out.println("File size: " + fileSize);

        conn.setAutoCommit(false);

        try (BufferedReader reader = new BufferedReader(new FileReader(indexFile, StandardCharsets.UTF_8))) {
            Matcher termMatcher = termPattern.matcher("");
            Matcher postingMatcher = postingPattern.matcher("");
            Matcher hitMatcher = hitPattern.matcher("");
            MatchResult lastTermMatchResult = null;
            MatchResult lastPostingMatchResult = null;

            int hitId = 1;
            int postingId = 1;
            int hitIdOffset = 1;
            int postingIdOffset = 1;

            long totalRead = 0;
            String line = null;
            while ((line = reader.readLine()) != null) {
                termMatcher = termMatcher.reset(line);
                postingMatcher = postingMatcher.reset(line);
                hitMatcher = hitMatcher.reset(line);

                if (termMatcher.find()) {
                    // Add the last term
                    if (lastTermMatchResult != null) {
                        try {
                            String term = lastTermMatchResult.group(1);
                            int df = Integer.parseInt(lastTermMatchResult.group(2));

                            pstmtTerm.setString(1, term);
                            pstmtTerm.setInt(2, df);
                            pstmtTerm.setInt(3, postingIdOffset);
                            pstmtTerm.addBatch();

                            postingIdOffset = postingId;
                        } catch (SQLException e) {
                            System.err.println("An error occured when batching Posting: " + e);
                        }
                    }
                    lastTermMatchResult = termMatcher.toMatchResult();
                }
                else if (postingMatcher.find()) {      
                    // Now we add postings for each term
                    if (lastPostingMatchResult != null) {           
                        try {
                            int docId = Integer.parseInt(lastPostingMatchResult.group(1));
                            int tf = Integer.parseInt(lastPostingMatchResult.group(2));

                            pstmtPosting.setInt(1, postingId++);
                            pstmtPosting.setInt(2, docId);
                            pstmtPosting.setInt(3, tf);
                            pstmtPosting.setInt(4, hitIdOffset);
                            pstmtPosting.addBatch();

                            hitIdOffset = hitId;
                        } catch (SQLException e) {
                            System.err.println("An error occured when batching Hit: " + e);
                        }
                    }
                    lastPostingMatchResult = postingMatcher.toMatchResult();
                }
                else if (hitMatcher.find()) {
                    int position = Integer.parseInt(hitMatcher.group(1));

                    try {
                        pstmtHit.setInt(1, hitId++);
                        pstmtHit.setInt(2, position);
                        pstmtHit.addBatch();
                    } catch (SQLException e) {
                        System.err.println("An error occured when batching Hit: " + e);
                    }                    
                }
                else {
                    System.err.println("Failed to parse line in index file: " + line);
                }

                if (totalRead % (10 * 1024) == 0) {
                    // pstmtHit.executeLargeBatch();
                    // pstmtPosting.executeLargeBatch();
                    // pstmtTerm.executeLargeBatch();
                    // conn.commit();
                    System.out.println("Progress ===> " + String.format("%.3f%%", (double)totalRead / fileSize * 100));
                }
                totalRead += line.length() + 1;
            }

            // Add the last term
            if (lastTermMatchResult != null) {
                try {
                    String term = lastTermMatchResult.group(1);
                    int df = Integer.parseInt(lastTermMatchResult.group(2));

                    pstmtTerm.setString(1, term);
                    pstmtTerm.setInt(2, df);
                    pstmtTerm.setInt(3, postingIdOffset);
                    pstmtTerm.addBatch();
                } catch (SQLException e) {
                    System.err.println("An error occured when batching Posting: " + e);
                }
            }

            pstmtHit.executeLargeBatch();
            pstmtPosting.executeLargeBatch();
            pstmtTerm.executeLargeBatch();
            conn.commit();
            System.out.println("Progress ===> " + String.format("%.3f%%", (double)totalRead / fileSize * 100));
        }

        pstmtTerm.close();
        pstmtPosting.close();
        pstmtHit.close();
        conn.setAutoCommit(true);
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Syntax: IndexUploader {index file} {database url}");
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

        try (Connection conn = DriverManager.getConnection(args[1], Config.DB_USER, Config.DB_PASS)) {
            System.err.println("Creating inverted index tables...");
            createInvertedIndexTables(conn);
            
            System.err.println("Uploading index file...");
            uploadIndexFile(conn, args[0]);

            System.err.println("Creating forward index table...");
            createForwardIndexTable(conn);

            System.err.println("Done!");
        } catch (Exception e) {
            System.err.println("An error occured: " + e);
        }
        
    }
}
