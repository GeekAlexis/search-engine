package edu.upenn.cis.cis455.search;

public class RankScore implements Comparable<RankScore> {
    private int docId;
    private double bm25;
    private double pageRank;
    private double score;

    public RankScore(int docId, double bm25, double pageRank, double score) {
        this.docId = docId;
        this.bm25 = bm25;
        this.pageRank = pageRank;
        this.score = score;
    }

    public int getDocId() {
        return docId;
    }

    public void setDocId(int docId) {
        this.docId = docId;
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
    public int compareTo(RankScore other) {
        return Double.compare(this.score, other.score);
    }

    @Override
    public String toString() {
        return "RankScore [bm25=" + bm25 + ", docId=" + docId + ", pageRank=" + pageRank + ", score=" + score + "]";
    }

}
