package storage;

public interface StorageInterface {

    /**
     * How many documents so far?
     */
    public int getCorpusSize();

    /**
     * Add a new document
     */
    public void addDocument(String url, String documentContents);

    /**
     * Retrieves a document's contents by URL
     */
    public String getDocument(String url);

    /**
     * Retrieves a document's crawled time by URL in System.currentTimeMillis()
     */
    public long getCrawledTime(String url);

    /**
     * Shuts down / flushes / closes the storage system
     */
    public void close();

    /**
     * Check if content is seen
     */
    public boolean checkSeenContent(String content);

	
}
