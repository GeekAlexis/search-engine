package indexer.storage;

import java.util.ArrayList;
import java.util.List;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;


@Entity
public class HitList {
    
    @PrimaryKey
    private int docId;

    private int termFreq = 0;

    private List<Integer> hits = new ArrayList<>();

    public HitList(int docId) {
        this.docId = docId;
    }

    public HitList() {}

    public int getDocId() {
        return docId;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }

    public int getTermFreq() {
        return termFreq;
    }

    public void incTermFreq() {
        this.termFreq++;
    }

    public List<Integer> getHits() {
        return hits;
    }

    public void setHits(List<Integer> hits) {
        this.hits = hits;
    }

    public void add(int hit) {
        hits.add(hit);
    }

    public int size() {
        return hits.size();
    }

    @Override
    public String toString() {
        return "HitList [docId=" + docId + ", hits=" + hits + "]";
    }

}
