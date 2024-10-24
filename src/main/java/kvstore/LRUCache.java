package kvstore;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private final int capacity;

    public LRUCache(int capacity) {
        super(capacity, 0.75f, true);
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be greater than 0");
        }
        this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        boolean shouldRemove = size() > capacity;
        if (shouldRemove) {
            System.out.println("Removing eldest entry: " + eldest);
        }
        return shouldRemove;
    }

    // Optional: A synchronized version of the LRUCache
    public static <K, V> Map<K, V> synchronizedLRUCache(int capacity) {
        return Collections.synchronizedMap(new LRUCache<>(capacity));
    }
}
