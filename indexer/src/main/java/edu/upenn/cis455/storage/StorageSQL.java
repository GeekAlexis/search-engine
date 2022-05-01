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

    static final String USER = "postgres";
    static final String PASS = "cis555db";

    private Connection dbConn = null;

    public StorageSQL(String url) {
        logger.debug("Opening SQL database URL: " + url);

        // // Get credentials from env variables
        // String user = System.getProperty("DATABASE_USER");
        // String password = System.getProperty("DATABASE_PASS");

        try {
            Class.forName("org.postgresql.Driver");
            dbConn = DriverManager.getConnection(url, USER, PASS);
        } catch (ClassNotFoundException | SQLException e) {
            logger.error("Failed to open database:", e);
        }

        logger.debug("Opened database successfully");
    }

    /**
     * Shuts down / flushes / closes the storage system
     */
    @Override
    public void close() throws SQLException {
        dbConn.close();
    }
    
}
