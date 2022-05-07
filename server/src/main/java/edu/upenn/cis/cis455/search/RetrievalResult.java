package edu.upenn.cis.cis455.search;

public class RetrievalResult {
    private String url;
    private String baseUrl;
    private String path;
    private String title;
    private String excerpt;
    private double bm25;
    private double pageRank;
    private double score;

    public RetrievalResult(double bm25, double pageRank, double score) {
        this.bm25 = bm25;
        this.pageRank = pageRank;
        this.score = score;
    }

    public RetrievalResult(String url, String baseUrl, String path, String title,
                           String excerpt, double bm25, double pageRank, double score) {
        this.url = url;
        this.baseUrl = baseUrl;
        this.path = path;
        this.title = title;
        this.excerpt = excerpt;
        this.bm25 = bm25;
        this.pageRank = pageRank;
        this.score = score;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getExcerpt() {
        return excerpt;
    }

    public void setExcerpt(String excerpt) {
        this.excerpt = excerpt;
    }

    public double getBm25() {
        return bm25;
    }

    public void setBm25(double bm25) {
        this.bm25 = bm25;
    }

    public double getPageRank() {
        return pageRank;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "RetrievalResult [baseUrl=" + baseUrl + ", bm25=" + bm25 + ", excerpt=" + excerpt + ", pageRank="
                + pageRank + ", path=" + path + ", score=" + score + ", title=" + title + ", url=" + url + "]";
    }
    
    
}
