package edu.upenn.cis.cis455;

public class DocOccurrence {
    private int docId;
    private int dl;
    private double pageRank;

    public DocOccurrence(int docId, int dl, double pageRank) {
        this.docId = docId;
        this.dl = dl;
        this.pageRank = pageRank;
    }

    public int getDocId() {
        return docId;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }

    public int getDl() {
        return dl;
    }

    public void setDl(int dl) {
        this.dl = dl;
    }

    public double getPageRank() {
        return pageRank;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
    }

    @Override
    public String toString() {
        return "DocOccurrence [docId=" + docId + ", dl=" + dl + ", pageRank=" + pageRank + "]";
    }

    
    
}
