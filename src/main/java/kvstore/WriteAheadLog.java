package kvstore;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class WriteAheadLog {
    private static final String LOG_FILE = "wal.log";
    private List<String> logs = new ArrayList<>();

    public WriteAheadLog() throws IOException {
        File file = new File(LOG_FILE);
        if (file.exists()) {
            recoverLog();
        } else {
            file.createNewFile();
        }
    }

    public synchronized void logOperation(String operation) throws IOException {
        logs.add(operation);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE, true))) {
            writer.write(operation + "\n");
        }
    }

    public List<String> recoverLog() throws IOException {
        List<String> recoveryOps = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(LOG_FILE))) {
            String line;
            while ((line = reader.readLine()) != null) {
                recoveryOps.add(line);
            }
        }
        return recoveryOps;
    }

    public synchronized void clearLog() throws IOException {
        Files.write(Paths.get(LOG_FILE), new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
        logs.clear();
    }
}
