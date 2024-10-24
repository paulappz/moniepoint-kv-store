package kvstore;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * LRUCache class implements a Least Recently Used (LRU) cache by extending LinkedHashMap.
 * It automatically removes the least recently accessed entry when the cache exceeds its defined capacity.
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private final int capacity;  // Maximum capacity of the cache

    public LRUCache(int capacity) {
        // LinkedHashMap constructor arguments: initial capacity, load factor (0.75), accessOrder (true for LRU ordering)
        super(capacity, 0.75f, true);
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be greater than 0");
        }
        this.capacity = capacity; // Set the cache capacity
    }

    /**
     * This method determines whether the eldest entry should be removed when a new entry is added.
     * It is overridden from LinkedHashMap to implement the LRU logic.
     */
    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        // Remove the eldest entry if the cache size exceeds the defined capacity
        boolean shouldRemove = size() > capacity;
        if (shouldRemove) {
            System.out.println("Removing eldest entry: " + eldest); // Log the removal
        }
        return shouldRemove;
    }
}
