package storage;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class StorageImpl implements StorageInterface {

    private Connection dbConn;

    public StorageImpl(Connection dbConn){
        this.dbConn = dbConn;
    }

    @Override
    public int getCorpusSize() {
        //TODO
        return 0;
    }

    @Override
    public void addDocument(String url, String content) {
//        Statement stmt = null;
        try {
            long crawledOn = System.currentTimeMillis();
//            byte[] contentBytes = content.getBytes();
//            stmt = dbConn.createStatement();
            String stm = "INSERT INTO \"Document\" (url, content, crawled_on) " +
                    "VALUES (?,?,?);";
            PreparedStatement pst = dbConn.prepareStatement(stm);
            pst.setString(1, url);
            pst.setBytes(2, content.getBytes());
            pst.setLong(3, crawledOn);
            pst.executeUpdate();



//            stmt.executeUpdate(sql);

            System.out.println("adding url: " + url);

            pst.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
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
            String sql = "SELECT crawled_on FROM \"Document\" WHERE url = '" + url + "';";

            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                BigDecimal crawledOn = rs.getBigDecimal("crawled_on");
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
        try {
            dbConn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
