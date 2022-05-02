package edu.upenn.cis455.storage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StorageSQL implements StorageInterface, AutoCloseable {
    final static Logger logger = LogManager.getLogger(StorageSQL.class);

    private static final String USER = "postgres";
    private static final String PASS = "cis555db";

    private Connection conn = null;

    /**
     * 
     * @param url
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public StorageSQL(String url) throws ClassNotFoundException, SQLException {
        logger.debug("Opening SQL database URL: " + url);
        
        // // Get credentials from env variables
        // String user = System.getProperty("DATABASE_USER");
        // String pass = System.getProperty("DATABASE_PASS");

        Class.forName("org.postgresql.Driver");
        conn = DriverManager.getConnection(url, USER, PASS);
    }

    /**
     * Shuts down / flushes / closes the storage system
     */
    @Override
    public void close() throws SQLException {
        conn.close();
    }
    
}
