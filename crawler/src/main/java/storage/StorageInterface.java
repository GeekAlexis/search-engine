package storage;

import java.util.HashMap;

public interface StorageInterface {

    /**
     * How many documents so far?
     */
    public int getCorpusSize();

    /**
     * Adds a new document
     */
    public void addDocument(String url, String documentContents);

    /**
     * Retrieves a document's contents by URL
     */
    public String getDocumentByUrl(String url);

    /**
     * Retrieves a document's crawled time by URL in System.currentTimeMillis()
     */
    public long getCrawledTime(String url);

    /**
     * Shuts down / flushes / closes the storage system
     */
    public void close();

    /**
     * Checks if content is seen
     */
    public boolean checkSeenContent(String content);

    /**
     * Gets all documents within a range
     */
    public HashMap<Integer, String> getDocumentByRange(int startIdx, int numDoc);

	
}
