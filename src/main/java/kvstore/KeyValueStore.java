package kvstore;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.NoSuchElementException;


public class KeyValueStore {
    private static final String DATA_FILE = "storage.txt"; // Persistent storage file
    private Map<String, String> store;

    public KeyValueStore() throws IOException {
        store = new HashMap<>();
        loadFromFile(); // Load existing data from file when initialized
    }

    // PUT method for inserting or updating keys
    public void put(String key, String value) throws IOException {
        validateKeyValue(key, value);
        store.put(key, value); // Add or update the key in the store
        saveToFile(); // Save the updated store to the file
        System.out.println("Inserted/Updated key: " + key);
    }

    // READ method for fetching a key's value
    public String get(String key) {
        return store.getOrDefault(key, "ERROR: Key not found");
    }

    // READ method for fetching key-value pairs in a range
    public List<String[]> readKeyRange(String startKey, String endKey) {
        return store.entrySet().stream()
                .filter(entry -> entry.getKey().compareTo(startKey) >= 0 && entry.getKey().compareTo(endKey) <= 0)
                .map(entry -> new String[]{entry.getKey(), entry.getValue()})
                .collect(Collectors.toList());
    }

    public void batchPut(Map<String, String> keyValuePairs) throws IOException {
        for (Map.Entry<String, String> entry : keyValuePairs.entrySet()) {
            validateKeyValue(entry.getKey(), entry.getValue());
            store.put(entry.getKey(), entry.getValue());
        }
        saveToFile(); // Save the updated store to the file
        System.out.println("Batch Inserted/Updated keys: " + keyValuePairs.keySet());
    }

    // DELETE method for removing keys
    public void delete(String key) throws IOException {
        validateKey(key); // Validate key only, as value is not involved
        if (store.containsKey(key)) {
            store.remove(key); // Remove the key from the in-memory store
            saveToFile(); // Save the updated store to the file
            System.out.println("Deleted key: " + key);
        } else {
            throw new NoSuchElementException("ERROR: Key not found");
        }
    }

    // Save the key/value pairs to the persistent storage
    private void saveToFile() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(DATA_FILE))) {
            for (Map.Entry<String, String> entry : store.entrySet()) {
                writer.write(entry.getKey() + "=" + entry.getValue());
                writer.newLine();
            }
        } catch (IOException e) {
            System.err.println("Error saving data to file: " + e.getMessage());
        }
    }

    // Load existing key/value pairs from the persistent storage
    private void loadFromFile() {
        try {
            Files.lines(Paths.get(DATA_FILE)).forEach(line -> {
                String[] parts = line.split("=", 2);
                if (parts.length >= 2) {
                    store.put(parts[0], parts[1]);
                }
            });
        } catch (IOException e) {
            System.err.println("Error loading data from file: " + e.getMessage());
        }
    }

    // Validate key only
    private void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
    }

    // Validate both key and value
    private void validateKeyValue(String key, String value) {
        validateKey(key); // Reuse key validation
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
    }
}
