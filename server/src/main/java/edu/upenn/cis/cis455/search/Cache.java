package edu.upenn.cis.cis455.search;

import java.util.LinkedHashMap;
import java.util.Map;

public class Cache<K, V> extends LinkedHashMap<K, V> {
    private int maxSize = 1000;

    public Cache(int maxSize) {
        super();
        this.maxSize = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(final Map.Entry eldest) {
        return size() > maxSize;
    }
}
