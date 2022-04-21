package storage;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class StorageImpl implements StorageInterface {

    private Connection dbConn;
    private int currId ;

    public StorageImpl(Connection dbConn){
        this.dbConn = dbConn;
        this.currId = getCurrIdFromFile();
    }

    @Override
    public int getCorpusSize() {
        return 0;
    }

    @Override
    public int addDocument(String url, String content) {
        Statement stmt = null;
        try {
            long crawledOn = System.currentTimeMillis();
            byte[] contentBytes = content.getBytes();
            stmt = dbConn.createStatement();
            int id = currId;
            currId += 1;
            String sql = "INSERT INTO \"Document\" (id, url, content, lastcrawl) " +
                    "VALUES (" + id + ", '" + url + "', '" + contentBytes + "', " +  crawledOn + ");";
            stmt.executeUpdate(sql);

            System.out.println("adding url: " + url);

            stmt.close();
            return id;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public String getDocument(String url) {
        Statement stmt = null;
        try {
            stmt = dbConn.createStatement();
            String sql = "SELECT content FROM \"Document\" WHERE url = '" + url + "';";

            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                byte[] contentByte = rs.getBytes("content");
                String content = new String(contentByte);
                return content;
            }
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public long getCrawledTime(String url) {
        Statement stmt = null;
        try {
            stmt = dbConn.createStatement();
            String sql = "SELECT lastcrawl FROM \"Document\" WHERE url = '" + url + "';";

            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                BigDecimal crawledOn = rs.getBigDecimal("lastcrawl");
                return crawledOn.longValue();
            }
            stmt.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public boolean checkSeen(String content) {
        Statement stmt = null;
        try {
            byte[] contentBytes = content.getBytes();
            stmt = dbConn.createStatement();
            String sql = "SELECT COUNT(1) AS num FROM \"Document\" WHERE content = '" + contentBytes + "';";

            ResultSet rs = stmt.executeQuery(sql);
            while ( rs.next() ) {
                int num = rs.getInt("num");
                if (num != 0){
                    return true;
                } else{
                    return false;
                }
            }
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public void close() {
        currId += 1;
        saveCurrIdToFile(currId);
        try {
            dbConn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    // Retrieve current Id from saved file dbId.txt
    public int getCurrIdFromFile(){
        int id = 0;
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader("./dbId.txt"));
            String line = reader.readLine();
            String idStr = line.split(":")[1];
            id = Integer.valueOf(idStr);
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return id;
    }

    // Save current Id to file dbId.txt
    public void saveCurrIdToFile(int id){
        System.out.println("saving current id");
        try {
            PrintWriter writer = new PrintWriter(new FileWriter("./dbId.txt"));
            writer.println("id:" + id);
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
