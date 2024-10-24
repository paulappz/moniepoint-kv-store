package com.kvstore;


import kvstore.KeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.NoSuchElementException; // Add this import


import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class KeyValueStoreTest {
    private KeyValueStore kvStore;

    @BeforeEach
    void setUp() throws IOException {
        kvStore = new KeyValueStore();
    }

    @Test
    void testPutAndGet() throws IOException {
        kvStore.put("key1", "value1");
        assertEquals("value1", kvStore.get("key1"));
    }

    @Test
    void testDelete() throws IOException {
        kvStore.put("key2", "value2");
        kvStore.delete("key2");

        // Expect a NoSuchElementException when attempting to access a deleted key
        assertThrows(NoSuchElementException.class, () -> {
            kvStore.get("key2");
        });
    }

    @Test
    void testBatchPut() throws IOException {
        kvStore.batchPut(Arrays.asList("key3", "key4"), Arrays.asList("value3", "value4"));
        assertEquals("value3", kvStore.get("key3"));
        assertEquals("value4", kvStore.get("key4"));
    }

    @Test
    void testReadKeyRange() throws IOException {
        kvStore.put("keyA", "valueA");
        kvStore.put("keyB", "valueB");
        kvStore.put("keyC", "valueC");
        kvStore.delete("keyB");  // This key will be tombstoned

        assertEquals(Arrays.asList("keyA", "keyC"), kvStore.readKeyRange("keyA", "keyC"));
    }

}
