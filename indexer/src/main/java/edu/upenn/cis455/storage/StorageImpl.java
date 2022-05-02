package edu.upenn.cis455.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.DatabaseException;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StorageImpl implements StorageInterface, AutoCloseable {
    final static Logger logger = LogManager.getLogger(StorageImpl.class);
    
    private Environment env;
    private List<EntityStore> stores = new ArrayList<>();

    /**
     * Constructor
     * @param directory
     * @throws DatabaseException
     * @throws FileNotFoundException
     */
    public StorageImpl(String directory) throws DatabaseException, FileNotFoundException {
        logger.debug("Opening BDB directory in: " + directory);

        // Create environment
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        env = new Environment(new File(directory), envConfig);
    }

    @Override
    public Map<Integer, HitList> createHitBuffer(int executorId) {
        // Create new entity store
        StoreConfig storeConfig = new StoreConfig();
        storeConfig.setAllowCreate(true);
        storeConfig.setTemporary(true);
        EntityStore newStore = new EntityStore(env, String.valueOf(executorId), storeConfig);
        stores.add(newStore);
        return newStore.getPrimaryIndex(Integer.class, HitList.class).sortedMap();
    }

    /**
     * Shuts down / flushes / closes the storage system
     */
    @Override
    public void close() throws DatabaseException {
        stores.forEach(store -> store.close());
        env.close();
    }

}