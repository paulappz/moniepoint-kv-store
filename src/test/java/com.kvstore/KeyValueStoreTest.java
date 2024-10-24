package com.kvstore;

import kvstore.KeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class KeyValueStoreTest {
    private KeyValueStore primaryStore;
    private KeyValueStore secondaryStore1;
    private KeyValueStore secondaryStore2;

    @BeforeEach
    void setUp() throws IOException {
        // Initialize key-value stores
        primaryStore = new KeyValueStore();
        secondaryStore1 = new KeyValueStore();
        secondaryStore2 = new KeyValueStore();

        // Add secondary stores to primary for replication
        primaryStore.addNode(secondaryStore1);
        primaryStore.addNode(secondaryStore2);
    }


    @Test
    void testPutAndGet() throws IOException {
        primaryStore.put("key1", "value1");
        assertEquals("value1", primaryStore.get("key1"));
    }

    @Test
    void testDelete() throws IOException {
        primaryStore.put("key2", "value2");
        primaryStore.delete("key2");

        // Expect a NoSuchElementException when attempting to access a deleted key
        assertThrows(NoSuchElementException.class, () -> {
            primaryStore.get("key2");
        });
    }

    @Test
    void testBatchPut() throws IOException {
        primaryStore.batchPut(Arrays.asList("key3", "key4"), Arrays.asList("value3", "value4"));
        assertEquals("value3", primaryStore.get("key3"));
        assertEquals("value4", primaryStore.get("key4"));
    }

    @Test
    void testReadKeyRange() throws IOException {
        primaryStore.put("keyA", "valueA");
        primaryStore.put("keyB", "valueB");
        primaryStore.put("keyC", "valueC");
        primaryStore.delete("keyB");  // This key will be tombstoned

        List<String[]> result = primaryStore.readKeyRange("keyA", "keyC");

        // Extract the keys from the result
        List<String> keys = result.stream()
                .map(entry -> entry[0])  // Extract the first element (key) from each string array
                .collect(Collectors.toList());

        assertEquals(Arrays.asList("keyA", "keyC"), keys);
    }

    @Test
    public void testReplicationToSecondaryNodes() throws IOException {
        // Insert key-value pair into primary store
        primaryStore.put("key1", "value1");

        // Ensure replication to secondary nodes
        assertEquals("value1", secondaryStore1.get("key1"));
        assertEquals("value1", secondaryStore2.get("key1"));
    }

    @Test
    public void testNodeFailureAndRecovery() throws IOException {
        // Simulate primary node failure
        primaryStore.simulateFailure();

        // Insert a key-value pair into secondary store during primary failure
        try {
            primaryStore.put("key2", "value2");
            fail("Expected IllegalStateException due to primary node failure.");
        } catch (IllegalStateException e) {
            assertEquals("Node is not active. Cannot process PUT operation.", e.getMessage());
        }

        // Simulate recovery of primary node
        primaryStore.recoverFromFailure();

        // Perform another operation after recovery
        primaryStore.put("key3", "value3");
        assertEquals("value3", primaryStore.get("key3"));

        // Ensure replication after recovery
        assertEquals("value3", secondaryStore1.get("key3"));
        assertEquals("value3", secondaryStore2.get("key3"));
    }

    @Test
    public void testFailoverHandling() throws IOException {
        // Simulate failure of primary node
        primaryStore.simulateFailure();

        // Test that secondary nodes can still process operations
        try {
            secondaryStore1.get("key1");
        } catch (IllegalStateException e) {
            fail("Secondary nodes should not fail when the primary node is down.");
        }

        // Test failover: promote one secondary node to primary
        primaryStore.setPrimaryNode(secondaryStore1);  // Promote secondaryStore1 to primary

        // Perform operations on the new primary
        secondaryStore1.put("key4", "value4");

        // Ensure the new primary replicates to other nodes
        assertEquals("value4", secondaryStore2.get("key4"));
        assertEquals("value4", secondaryStore1.get("key4"));
    }

    @Test
    public void testReplicationAfterNodeRecovery() throws IOException {
        // Insert key-value pair into primary store
        primaryStore.put("key5", "value5");

        // Simulate failure of secondary node
        secondaryStore1.simulateFailure();

        // Perform more operations on the primary node
        primaryStore.put("key6", "value6");

        // Simulate recovery of secondary node
        secondaryStore1.recoverFromFailure();

        // Ensure that secondary node has synced with the primary node after recovery
        assertEquals("value5", secondaryStore1.get("key5"));
        assertEquals("value6", secondaryStore1.get("key6"));
    }

    @Test
    public void testBatchPutAndReplication() throws IOException {
        // Prepare batch data
        List<String> keys = Arrays.asList("batchKey1", "batchKey2", "batchKey3");
        List<String> values = Arrays.asList("batchValue1", "batchValue2", "batchValue3");

        // Perform batch put operation on primary
        primaryStore.batchPut(keys, values);

        // Ensure all keys are replicated to secondary nodes
        for (int i = 0; i < keys.size(); i++) {
            assertEquals(values.get(i), secondaryStore1.get(keys.get(i)));
            assertEquals(values.get(i), secondaryStore2.get(keys.get(i)));
        }
    }
}
