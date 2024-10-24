package kvstore;

import java.io.*;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.util.Map;

public class SSTable {
    private File file;
    private TreeMap<String, String> data;  // In-memory cache of the SSTable's data

    private SSTable(File file, TreeMap<String, String> data) {
        this.file = file;
        this.data = data;
    }

    /**
     * Creates an SSTable from the given memTable and writes it to disk.
     */
    public static SSTable createFromMemTable(TreeMap<String, String> memTable) throws IOException {
        File file = new File(UUID.randomUUID().toString() + ".sstable");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            for (Map.Entry<String, String> entry : memTable.entrySet()) {
                writer.write(entry.getKey() + ":" + entry.getValue() + "\n");
            }
        }
        // Create an SSTable instance with an in-memory copy of the data
        return new SSTable(file, new TreeMap<>(memTable));
    }

    /**
     * Reads a value for a given key from the in-memory data of this SSTable.
     */
    public String read(String key) {
        return data.get(key);
    }

    /**
     * Retrieves all keys within the given key range [startKey, endKey], excluding deleted entries.
     */
    public List<String> getKeyRange(String startKey, String endKey) throws IOException {
        // Get a submap for the range and filter out keys with null values (i.e., deleted entries)
        Map<String, String> rangeMap = data.subMap(startKey, true, endKey, true);
        List<String> validKeys = new ArrayList<>();

        for (Map.Entry<String, String> entry : rangeMap.entrySet()) {
            if (entry.getValue() != null) {  // Exclude deleted entries
                validKeys.add(entry.getKey());
            }
        }

        return validKeys;
    }

    /**
     * Loads the SSTable data from disk into memory (used when loading existing SSTables).
     */
    public static SSTable loadFromFile(File file) throws IOException {
        TreeMap<String, String> data = new TreeMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(":");
                if (parts.length == 2) {
                    data.put(parts[0], parts[1]);
                } else {
                    data.put(parts[0], null);  // Treat entries with missing values as deletions
                }
            }
        }

        return new SSTable(file, data);
    }

    public File getFile() {
        return file;
    }
}
