package edu.upenn.cis.cis455;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
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
                          "id bigint PRIMARY KEY," +
                          "position integer not null)";

        String postingTable = "CREATE TABLE \"Posting\" (" +
                              "id bigint PRIMARY KEY," +
                              "doc_id integer REFERENCES \"Document\" (id)," +
                              "tf integer," +
                              "hit_id_offset bigint REFERENCES \"Hit\" (id))";

        String lexiconTable = "CREATE TABLE \"Lexicon\" (" +
                              "id serial PRIMARY KEY," +
                              "term text not null UNIQUE," +
                              "df integer," +
                              "posting_id_offset bigint REFERENCES \"Posting\" (id))";

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
        Pattern termPattern = Pattern.compile("^(.{1,100}),(\\d{1,10}):$", Pattern.DOTALL);
        Pattern postingPattern = Pattern.compile("^<(\\d{1,10}),(\\d{1,10})>$");
        Pattern hitPattern = Pattern.compile("^(\\d{1,10});$");

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

            long hitId = 1;
            long hitIdOffset = 1;
            long postingId = 1;
            long postingIdOffset = 1;

            long totalRead = 0;
            String line = null;
            while ((line = reader.readLine()) != null) {
                termMatcher = termMatcher.reset(line);
                postingMatcher = postingMatcher.reset(line);
                hitMatcher = hitMatcher.reset(line);

                if (termMatcher.find()) {
                    // Add the last term
                    if (lastPostingMatchResult != null) {           
                        int docId = Integer.parseInt(lastPostingMatchResult.group(1));
                        int tf = Integer.parseInt(lastPostingMatchResult.group(2));

                        pstmtPosting.setLong(1, postingId++);
                        pstmtPosting.setInt(2, docId);
                        pstmtPosting.setInt(3, tf);
                        pstmtPosting.setLong(4, hitIdOffset);
                        pstmtPosting.addBatch();
                        // System.out.println("docId: " + docId + " hitIdOffset: " + hitIdOffset);

                        hitIdOffset = hitId;
                    }
                    if (lastTermMatchResult != null) {
                        String term = lastTermMatchResult.group(1);
                        int df = Integer.parseInt(lastTermMatchResult.group(2));

                        pstmtTerm.setString(1, term);
                        pstmtTerm.setInt(2, df);
                        pstmtTerm.setLong(3, postingIdOffset);
                        pstmtTerm.addBatch();
                        // System.out.println("term: " + term + " postingIdOffset: " + postingIdOffset);

                        postingIdOffset = postingId;
                    }
                    lastTermMatchResult = termMatcher.toMatchResult();
                    lastPostingMatchResult = null;
                }
                else if (postingMatcher.find()) {      
                    // Now we add postings for each term
                    if (lastPostingMatchResult != null) {           
                        int docId = Integer.parseInt(lastPostingMatchResult.group(1));
                        int tf = Integer.parseInt(lastPostingMatchResult.group(2));

                        pstmtPosting.setLong(1, postingId++);
                        pstmtPosting.setInt(2, docId);
                        pstmtPosting.setInt(3, tf);
                        pstmtPosting.setLong(4, hitIdOffset);
                        pstmtPosting.addBatch();
                        // System.out.println("docId: " + docId + " hitIdOffset: " + hitIdOffset);

                        hitIdOffset = hitId;
                    }
                    lastPostingMatchResult = postingMatcher.toMatchResult();
                }
                else if (hitMatcher.find()) {
                    int position = Integer.parseInt(hitMatcher.group(1));

                    pstmtHit.setLong(1, hitId++);
                    pstmtHit.setInt(2, position);
                    pstmtHit.addBatch();              
                }
                else {
                    System.err.println("Failed to parse line in index file: " + line);
                }

                if (totalRead % (8 * 1024) == 0) {
                    System.out.println("Progress ===> " + String.format("%.3f%%", (double)totalRead / fileSize * 100));
                    pstmtHit.executeLargeBatch();
                    pstmtPosting.executeLargeBatch();
                    pstmtTerm.executeLargeBatch();
                    conn.commit();
                }
                totalRead += line.length() + 1;
            }

            // Add the last posting and term
            if (lastPostingMatchResult != null) {           
                int docId = Integer.parseInt(lastPostingMatchResult.group(1));
                int tf = Integer.parseInt(lastPostingMatchResult.group(2));

                pstmtPosting.setLong(1, postingId++);
                pstmtPosting.setInt(2, docId);
                pstmtPosting.setInt(3, tf);
                pstmtPosting.setLong(4, hitIdOffset);
                pstmtPosting.addBatch();
            }
            if (lastTermMatchResult != null) {
                String term = lastTermMatchResult.group(1);
                int df = Integer.parseInt(lastTermMatchResult.group(2));

                pstmtTerm.setString(1, term);
                pstmtTerm.setInt(2, df);
                pstmtTerm.setLong(3, postingIdOffset);
                pstmtTerm.addBatch();
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
        if (args.length != 1) {
            System.err.println("Syntax: IndexUploader {index file}");
            System.exit(1);
        }

        String dbUrl = null;
        String dbUser = null;
        String dbPass = null;
        try (InputStream in = IndexUpload.class.getClassLoader().getResourceAsStream("config.properties")) {
            Properties prop = new Properties();
            prop.load(in);
            dbUrl = prop.getProperty("db.url");
            dbUser = prop.getProperty("db.user");
            dbPass = prop.getProperty("db.pass");
        } catch (IOException e) {
            System.err.println("An error occured: " + e);
        }

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("Failed to find database driver: " + e);
        }

        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass)) {
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
