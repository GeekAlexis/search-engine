package indexer.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StorageFactory {
    final static Logger logger = LogManager.getLogger(StorageFactory.class);

    public static StorageInterface getDatabaseInstance(String directory) throws Exception {
        return new StorageImpl(directory);
    }
}

