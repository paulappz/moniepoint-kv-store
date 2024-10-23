package kvstore;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

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

    // Save the key/value pairs to the persistent storage
    private void saveToFile() {
        try (FileWriter writer = new FileWriter(DATA_FILE)) {
            for (Map.Entry<String, String> entry : store.entrySet()) {
                writer.write(entry.getKey() + "=" + entry.getValue() + "\n");
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
                    String key = parts[0];
                    String value = parts[1];
                    store.put(key, value);
                }
            });
        } catch (IOException e) {
            System.err.println("Error loading data from file: " + e.getMessage());
        }
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
}
