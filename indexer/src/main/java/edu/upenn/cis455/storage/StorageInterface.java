package edu.upenn.cis455.storage;

import java.util.Map;

public interface StorageInterface {
    Map<Integer, HitList> createHitBuffer(int executorId);
}
