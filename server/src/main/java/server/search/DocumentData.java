package server.search;

public class DocumentData {
    private int dl;
    private double pageRank;

    public DocumentData(int dl, double pageRank) {
        this.dl = dl;
        this.pageRank = pageRank;
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
        return "DocumentData [dl=" + dl + ", pageRank=" + pageRank + "]";
    }
    
}
