package indexer.storage;

import java.util.Map;

public interface StorageInterface {
    Map<Integer, HitList> createHitBuffer(int executorId);
}
