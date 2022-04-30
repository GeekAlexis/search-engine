package edu.upenn.cis455.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ParserWritable implements Writable {
    private String term;
    private long docId;
    private long pos;

    public ParserWritable() {}

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBytes(term);
        out.writeLong(docId);
        out.writeLong(pos);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        term = in.readUTF();
        docId = in.readLong();
        pos = in.readLong();
        
    }

    public static ParserWritable read(DataInput in) throws IOException {
        ParserWritable w = new ParserWritable();
        w.readFields(in);
        return w;
    }
}
