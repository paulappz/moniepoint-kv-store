package kvstore;

import java.io.IOException;
import java.util.*;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class KeyValueStore {
    private WriteAheadLog wal;
    private LSMTree lsmTree;
    private LRUCache<String, String> cache;
    private List<KeyValueStore> nodes; // List of nodes for replication
    private KeyValueStore primaryNode; // Track the primary node
    private boolean isActive; // Simulate if the current node is active or failed

    // Store for the key-value pairs
    private Map<String, String> store;

    public KeyValueStore() throws IOException {
        wal = new WriteAheadLog();
        lsmTree = new LSMTree();
        cache = new LRUCache<>(100);
        nodes = new ArrayList<>(); // Initialize the list of nodes
        isActive = true; // Initially, the node is active
        store = new HashMap<>();

        // Recover from WAL
        recoverFromWAL();
    }

    // Recover from Write Ahead Log (WAL)
    private void recoverFromWAL() throws IOException {
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

    // Method to add nodes to the cluster
    public void addNode(KeyValueStore store) {
        nodes.add(store); // Add a secondary node for replication
    }

    // Method to set the primary node
    public void setPrimaryNode(KeyValueStore store) {
        this.primaryNode = store; // Set the primary node using the passed parameter
    }

    // Simulate failure of this node
    public void simulateFailure() {
        this.isActive = false;
        System.out.println("Node has failed.");
    }

    // Simulate recovery of this node
    public void recoverFromFailure() {
        this.isActive = true;
        syncWithPrimary(); // Sync with the current primary node on recovery
        System.out.println("Node has recovered and synced with the primary.");
    }

    // Check if the current node is active
    public boolean isActive() {
        return this.isActive;
    }

    // PUT method for inserting or updating keys with replication
    public void put(String key, String value) throws IOException {
        if (!this.isActive) {
            throw new IllegalStateException("Node is not active. Cannot process PUT operation.");
        }

        validateKeyValue(key, value);
        wal.logOperation("PUT:" + key + ":" + value);
        cache.put(key, value);
        lsmTree.put(key, value); // Add or update the key in LSM Tree

        // Replicate the data to other nodes
        for (KeyValueStore node : nodes) {
            if (node.isActive()) {
                replicatePut(node, key, value);
            }
        }
        System.out.println("Inserted/Updated key: " + key);
    }

    // Replicate PUT operation to a secondary node
    private void replicatePut(KeyValueStore node, String key, String value) throws IOException {
        System.out.println("Replicating to node: PUT " + key + " = " + value);
        node.put(key, value); // Directly call put on the KeyValueStore
    }

    // GET method for retrieving values by key with failover handling
    public String get(String key) {
        validateKey(key);

        // Check if the key exists in the cache
        if (cache.containsKey(key)) {
            String value = cache.get(key);
            if (value == null) {
                throw new NoSuchElementException("ERROR: Key not found or already deleted");
            }
            return value;
        }

        // Check if the key exists in the LSM tree
        try {
            if (lsmTree.containsKey(key)) {
                String value = lsmTree.get(key);
                if (value == null) {
                    throw new NoSuchElementException("ERROR: Key not found or already deleted");
                }
                return value;
            }
        } catch (Exception e) {
            System.err.println("ERROR: Exception in LSM tree operation: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("ERROR: An issue occurred while accessing the LSM tree");
        }

        // If not found, check the primary node during recovery
        if (!this.isActive && primaryNode != null && primaryNode.isActive()) {
            try {
                return primaryNode.getStore().get(key); // Fetch from the primary node
            } catch (NoSuchElementException e) {
                throw new NoSuchElementException("ERROR: Key not found in primary node as well");
            }
        }

        // Key doesn't exist in either cache, LSM tree, or primary node
        throw new NoSuchElementException("ERROR: Key not found");
    }

    // DELETE method for removing keys (records a tombstone)
    public boolean delete(String key) throws IOException {
        validateKey(key);

        if (!this.isActive) {
            throw new IllegalStateException("Node is not active. Cannot process DELETE operation.");
        }

        // Check if the key exists in the cache or LSM tree
        if (cache.containsKey(key) || lsmTree.containsKey(key)) {
            wal.logOperation("DELETE:" + key);
            cache.remove(key);
            lsmTree.put(key, null); // Mark the key as deleted (tombstone)

            // Replicate the deletion to other nodes
            for (KeyValueStore node : nodes) {
                if (node.isActive()) {
                    replicateDelete(node, key);
                }
            }

            System.out.println("Deleted key: " + key);
            return true;
        } else {
            throw new NoSuchElementException("ERROR: Key not found or already deleted");
        }
    }

    // Replicate DELETE operation to a secondary node
    private void replicateDelete(KeyValueStore node, String key) throws IOException {
        System.out.println("Replicating to node: DELETE " + key);
        node.delete(key);
    }

    // Sync with the current primary node after recovery
    private void syncWithPrimary() {
        if (primaryNode != null && primaryNode.isActive()) {
            System.out.println("Syncing with primary node...");

            // Fetch and update local store from primary node
            try {
                List<String[]> syncedData = primaryNode.readKeyRange("startKey", "endKey");
                for (String[] entry : syncedData) {
                    String key = entry[0];
                    String value = entry[1];
                    this.put(key, value); // Insert or update the key-value pair
                }
            } catch (IOException e) {
                System.err.println("ERROR during syncing: " + e.getMessage());
            }
        }
    }

    // Batch PUT method for inserting multiple key-value pairs from a Map
    public void batchPut(List<String> keys, List<String> values) throws IOException {
        if (!this.isActive) {
            throw new IllegalStateException("Node is not active. Cannot process batch PUT operation.");
        }

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

        // Fetch keys from the memTable in the specified range
        Map<String, String> memTableRange = lsmTree.getMemTable().subMap(startKey, true, endKey, true);
        memTableRange.forEach((key, value) -> {
            if (value != null) {
                combinedStore.putIfAbsent(key, value);
            }
        });

        // Iterate through SSTables to find keys in the specified range
        for (SSTable sstable : lsmTree.getSSTables()) {
            List<String> sstableKeys = sstable.getKeyRange(startKey, endKey);
            for (String key : sstableKeys) {
                if (!combinedStore.containsKey(key)) {
                    String value = sstable.read(key);
                    if (value != null) {
                        combinedStore.put(key, value);
                    }
                }
            }
        }

        // Return the combined key-value pairs in the specified range
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

    // Validate key
    private void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
    }

    // Get store for external access if needed
    public Map<String, String> getStore() {
        return store;
    }
}
