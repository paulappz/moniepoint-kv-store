package kvstore;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * The WriteAheadLog (WAL) class provides a mechanism for logging operations 
 * to persistent storage before they are applied to the main system, ensuring 
 * durability in the event of system failure.
 */
public class WriteAheadLog {
    private static final String LOG_FILE = "wal.log";  // The file where log entries are stored
    private List<String> logs = new ArrayList<>();     // In-memory list of log entries

    /**
     * Constructor for WriteAheadLog. If the log file already exists, it recovers the log entries;
     * otherwise, it creates a new log file.
     */
    public WriteAheadLog() throws IOException {
        File file = new File(LOG_FILE);
        if (file.exists()) {
            recoverLog(); // Recover log entries if the log file exists
        } else {
            file.createNewFile(); // Create a new log file if it doesn't exist
        }
    }

    /**
     * Synchronized method to log operations in a thread-safe manner.
     * Writes the operation to the log file and stores it in the in-memory list.
     */
    public synchronized void logOperation(String operation) throws IOException {
        logs.add(operation); // Add the operation to the in-memory list
        // Append the operation to the log file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE, true))) {
            writer.write(operation + "\n");
        }
    }

    /**
     * Recovers the log entries from the log file.
     * Reads the log file line by line and returns a list of logged operations.
     */
    public List<String> recoverLog() throws IOException {
        List<String> recoveryOps = new ArrayList<>(); // List to store recovered operations
        // Read the log file line by line
        try (BufferedReader reader = new BufferedReader(new FileReader(LOG_FILE))) {
            String line;
            while ((line = reader.readLine()) != null) {
                recoveryOps.add(line); // Add each log entry to the recovery list
            }
        }
        return recoveryOps; // Return the recovered operations
    }

    /**
     * Synchronized method to clear the log file.
     * This method truncates the log file and clears the in-memory list of logs.
     */
    public synchronized void clearLog() throws IOException {
        // Truncate the log file to empty its contents
        Files.write(Paths.get(LOG_FILE), new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
        logs.clear(); // Clear the in-memory list of logs
    }
}
