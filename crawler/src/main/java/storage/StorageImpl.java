package storage;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

import java.util.HashMap;

public class StorageImpl implements StorageInterface {

    private Connection dbConn;

    public StorageImpl(Connection dbConn){
        this.dbConn = dbConn;
    }

    @Override
    public int getCorpusSize() {
        Statement stmt = null;
        try {
            stmt = dbConn.createStatement();
            String sql = "SELECT COUNT(*) AS num FROM \"Document\";";

            ResultSet rs = stmt.executeQuery(sql);

            while ( rs.next() ) {
                int num = rs.getInt("num");
                return num;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public void addDocument(String url, String content) {
        // if url not in db yet
        if (getDocumentByUrl(url) == null){
            try {
                long crawledOn = System.currentTimeMillis();
                String stm = "INSERT INTO \"Document\" (url, content, crawled_on) " +
                        "VALUES (?,?,?);";
                PreparedStatement pst = dbConn.prepareStatement(stm);
                pst.setString(1, url);
                pst.setBytes(2, content.getBytes());
                pst.setLong(3, crawledOn);
                pst.executeUpdate();

                System.out.println("adding url: " + url);

                pst.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            // update existing record
        }
//        else {
//            try {
//                long crawledOn = System.currentTimeMillis();
//                String stm = "UPDATE \"Document\" SET content = ?, crawled_on = ? WHERE url = ?";
//
//                PreparedStatement pst = dbConn.prepareStatement(stm);
//                pst.setBytes(1, content.getBytes());
//                pst.setLong(2, crawledOn);
//                pst.setString(3, url);
//                pst.executeUpdate();
//
//                System.out.println("updating url: " + url);
//
//                pst.close();
//
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//
//        }
    }


    @Override
    public String getDocumentByUrl(String url) {
        Statement stmt = null;
        try {
            stmt = dbConn.createStatement();
            String sql = "SELECT content FROM \"Document\" WHERE url = '" + url + "';";

            ResultSet rs = stmt.executeQuery(sql);

            if (rs.next() == false){
                // Result set is empty
                stmt.close();
                return null;
            } else{
                do {
                    byte[] contentByte = rs.getBytes("content");
                    String content = new String(contentByte);
                    stmt.close();
                    return content;
                } while (rs.next());
            }

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
    public boolean checkSeenContent(String content) {
        if (content == null){
            return true;
        }
        try {
            String stm = "SELECT COUNT(1) AS num FROM \"Document\" WHERE content = ?;";

            PreparedStatement pst = dbConn.prepareStatement(stm);
            pst.setBytes(1, content.getBytes());

            ResultSet rs = pst.executeQuery();
            while ( rs.next() ) {
                int num = rs.getInt("num");
                if (num != 0){
                    return true;
                } else{
                    return false;
                }
            }
            pst.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public HashMap<Integer, String> getDocumentByRange(int startIdx, int numDoc){
        HashMap<Integer, String> map = new HashMap<>();

        try {
            String stm = "SELECT id, content FROM \"Document\" ORDER BY id LIMIT ? OFFSET ?;";
            PreparedStatement pst = dbConn.prepareStatement(stm);
            pst.setInt(1, numDoc);
            pst.setInt(2, startIdx - 1);

            ResultSet rs = pst.executeQuery();
            while ( rs.next() ) {
                int id = rs.getInt("id");
                byte[] contentByte = rs.getBytes("content");
                String content = new String(contentByte);
                map.put(id, content);
            }

            pst.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return map;
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
