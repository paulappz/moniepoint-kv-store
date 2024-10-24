package kvstore;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map;
import java.util.stream.Collectors;

public class LSMTree {
    private TreeMap<String, String> memTable = new TreeMap<>();
    private static final int MAX_MEMTABLE_SIZE = 100;
    private List<SSTable> sstables = new ArrayList<>();

    /**
     * Inserts a key-value pair into the memTable. If the value is null, the key is considered deleted.
     */
    public synchronized void put(String key, String value) throws IOException {
        memTable.put(key, value);
        if (memTable.size() >= MAX_MEMTABLE_SIZE) {
            flushMemTableToDisk();
        }
    }

    /**
     * Retrieves the value associated with the given key from either the memTable or SSTables.
     */
    public synchronized String get(String key) throws IOException {
        if (memTable.containsKey(key)) {
            return memTable.get(key);
        }
        for (SSTable sstable : sstables) {
            String value = sstable.read(key);
            if (value != null) return value;  // If value found in SSTable
        }
        return null;
    }

    public boolean containsKey(String key) throws IOException {
        // Check in memTable first
        if (memTable.containsKey(key) && memTable.get(key) != null) {
            return true;
        }
        // Check in SSTables
        for (SSTable sstable : sstables) {
            String value = sstable.read(key);
            if (value != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * Retrieves all keys within the given key range [startKey, endKey], excluding deleted keys.
     */
    public synchronized List<String> getKeyRange(String startKey, String endKey) throws IOException {
        // 1. Get all keys in the range from memTable and filter out null values (deleted keys)
        Map<String, String> rangeMap = memTable.subMap(startKey, true, endKey, true);
        List<String> validKeysInMemTable = rangeMap.entrySet().stream()
                .filter(entry -> entry.getValue() != null)  // Exclude deleted entries
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        // 2. Check SSTables for keys in the range, avoiding duplicates
        for (SSTable sstable : sstables) {
            List<String> sstableKeys = sstable.getKeyRange(startKey, endKey);
            for (String key : sstableKeys) {
                if (!memTable.containsKey(key)) {  // Avoid duplicates
                    String value = sstable.read(key);
                    if (value != null) {
                        validKeysInMemTable.add(key);  // Only add if not deleted
                    }
                }
            }
        }

        return validKeysInMemTable;
    }


    /**
     * Flushes the memTable to disk by creating a new SSTable.
     */
    private void flushMemTableToDisk() throws IOException {
        SSTable newTable = SSTable.createFromMemTable(memTable);
        sstables.add(newTable);
        memTable.clear();
    }
}
