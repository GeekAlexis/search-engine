package edu.upenn.cis.cis455.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ParserWritable implements WritableComparable<ParserWritable> {
    private String term;
    private int docId;
    private int pos;

    public ParserWritable(String term, int docId, int pos) {
        this.term = term;
        this.docId = docId;
        this.pos = pos;
    }

    public ParserWritable() {}

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(term);
        out.writeInt(docId);
        out.writeInt(pos);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        term = in.readUTF();
        docId = in.readInt();
        pos = in.readInt();
    }

    public static ParserWritable read(DataInput in) throws IOException {
        ParserWritable w = new ParserWritable();
        w.readFields(in);
        return w;
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public int getDocId() {
        return docId;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    @Override
    public int compareTo(ParserWritable other) {
        int compareVal = this.term.compareTo(other.term);
        if (compareVal == 0) {
            compareVal = Integer.compare(this.docId, other.docId);
        }
        if (compareVal == 0) {
            compareVal = Integer.compare(this.pos, other.pos);
        }
        return compareVal;
    }
}
