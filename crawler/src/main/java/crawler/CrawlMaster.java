package crawler;

public interface CrawlMaster {
    /**
     * We've indexed another document
     */
    public void incCount();

    /**
     * Workers can poll this to see if they should exit, ie the crawl is done
     */
    public boolean isDone();

    /**
     * Workers should notify when they are processing an URL
     */
    public void setWorking(boolean working);

    /**
     * Workers should call this when they exit, so the master knows when it can shut
     * down
     */
    public void notifyThreadExited();
}
