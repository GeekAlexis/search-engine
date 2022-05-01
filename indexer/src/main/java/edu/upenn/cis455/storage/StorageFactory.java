package edu.upenn.cis455.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StorageFactory {
    final static Logger logger = LogManager.getLogger(StorageFactory.class);

    public static Storage getDatabaseInstance(String directory) throws Exception {
        return new Storage(directory);
    }
}

