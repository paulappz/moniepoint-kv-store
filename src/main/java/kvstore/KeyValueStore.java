package kvstore;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.NoSuchElementException;


public class KeyValueStore {
    private WriteAheadLog wal;
    private LSMTree lsmTree;
    private LRUCache<String, String> cache;

    public KeyValueStore() throws IOException {
        wal = new WriteAheadLog();
        lsmTree = new LSMTree();
        cache = new LRUCache<>(100);

        // Recover from WAL
        List<String> recoveryOps = wal.recoverLog();
        for (String op : recoveryOps) {
            String[] parts = op.split(":");
            if ("DELETE".equals(parts[0])) {
                lsmTree.put(parts[1], null); // Record tombstone for deleted key
            } else if ("PUT".equals(parts[0])) {
                lsmTree.put(parts[1], parts[2]); // Restore key-value pairs
            }
        }
    }

    // PUT method for inserting or updating keys
    public void put(String key, String value) throws IOException {
        validateKeyValue(key, value);
        wal.logOperation("PUT:" + key + ":" + value);
        cache.put(key, value);
        lsmTree.put(key, value); // Add or update the key in LSM Tree
        System.out.println("Inserted/Updated key: " + key);
    }

    // GET method for retrieving values by key
    public String get(String key) throws IOException {
        validateKey(key);
        if (cache.containsKey(key)) {
            return cache.get(key);
        } else if (lsmTree.containsKey(key) && lsmTree.get(key) == null) {
            // Key was deleted, so throw a NoSuchElementException
            throw new NoSuchElementException("ERROR: Key not found or already deleted");
        } else {
            // Handle cases where the key doesn't exist in both cache and lsmTree
            throw new NoSuchElementException("ERROR: Key not found");
        }
    }

    // DELETE method for removing keys (records a tombstone)
    public boolean delete(String key) throws IOException {
        validateKey(key);

        // Check if the key exists in the cache or lsmTree
        if (cache.containsKey(key) || lsmTree.containsKey(key)) {
            wal.logOperation("DELETE:" + key);
            cache.remove(key);
            lsmTree.put(key, null); // Mark the key as deleted (tombstone)
            System.out.println("Deleted key: " + key);
            return true; // Indicate successful deletion
        } else {
            // Key doesn't exist; throw an exception
            throw new NoSuchElementException("ERROR: Key not found or already deleted");
        }
    }


    // Batch PUT method for inserting multiple key-value pairs from a Map
    public void batchPut(List<String> keys, List<String> values) throws IOException {
        if (keys.size() != values.size()) {
            throw new IllegalArgumentException("Keys and values must be of the same length.");
        }
        for (int i = 0; i < keys.size(); i++) {
            put(keys.get(i), values.get(i));
        }
    }

    // READ method for fetching key-value pairs in a range
    public List<String[]> readKeyRange(String startKey, String endKey) throws IOException {
        Map<String, String> combinedStore = new HashMap<>(cache);

        // Get key-value pairs from the LSMTree's memTable
        Map<String, String> memTableRange = lsmTree.getMemTable().subMap(startKey, true, endKey, true);
        memTableRange.forEach((key, value) -> {
            if (value != null) { // Exclude deleted keys (null values)
                combinedStore.putIfAbsent(key, value);
            }
        });

        // Get key-value pairs from SSTables, avoiding duplicates
        for (SSTable sstable : lsmTree.getSSTables()) {
            List<String> sstableKeys = sstable.getKeyRange(startKey, endKey);
            for (String key : sstableKeys) {
                if (!combinedStore.containsKey(key)) { // Avoid duplicates
                    String value = sstable.read(key);
                    if (value != null) {
                        combinedStore.put(key, value);
                    }
                }
            }
        }

        // Filter the combined entries within the range and return them as a list
        return combinedStore.entrySet().stream()
                .filter(entry -> entry.getKey().compareTo(startKey) >= 0 && entry.getKey().compareTo(endKey) <= 0)
                .map(entry -> new String[]{entry.getKey(), entry.getValue()})
                .collect(Collectors.toList());
    }


    // Validate key and value
    private void validateKeyValue(String key, String value) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
    }

    private void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
    }
}
