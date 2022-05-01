package edu.upenn.cis455.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StorageFactory {
    final static Logger logger = LogManager.getLogger(StorageFactory.class);

    public static StorageInterface getDatabaseInstance(String uri) throws Exception {
        if (uri.startsWith("jdbc")) {
            return new StorageSQL(uri);
        }
        else {
            return new StorageBDB(uri);
        }
    }
}

