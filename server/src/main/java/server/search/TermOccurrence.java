package server.search;

import java.util.List;

public class TermOccurrence {
    private int df;
    private List<Integer> docIds;
    private List<Integer> tfs;

    public TermOccurrence(int df, List<Integer> docIds, List<Integer> tfs) {
        this.df = df;
        this.docIds = docIds;
        this.tfs = tfs;
    }

    public int getDf() {
        return df;
    }

    public void setDf(int df) {
        this.df = df;
    }

    public List<Integer> getDocIds() {
        return docIds;
    }

    public void setDocIds(List<Integer> docIds) {
        this.docIds = docIds;
    }

    public List<Integer> getTfs() {
        return tfs;
    }

    public void setTfs(List<Integer> tfs) {
        this.tfs = tfs;
    }

    @Override
    public String toString() {
        return "TermOccurrence [df=" + df + ", docIds=" + docIds + ", tfs=" + tfs + "]";
    }
    
}
