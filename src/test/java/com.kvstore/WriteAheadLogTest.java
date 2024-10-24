package com.kvstore;

import kvstore.WriteAheadLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WriteAheadLogTest {
    private WriteAheadLog wal;

    @BeforeEach
    void setUp() throws IOException {
        wal = new WriteAheadLog();
    }

    @Test
    void testLogOperation() throws IOException {
        wal.logOperation("PUT:key1:value1");
        assertTrue(wal.recoverLog().contains("PUT:key1:value1"), "Log should contain the written operation");
    }

    @Test
    void testRecoverLog() throws IOException {
        wal.logOperation("PUT:key1:value1");
        wal.logOperation("DELETE:key2");

        List<String> logEntries = wal.recoverLog();

        assertTrue(logEntries.contains("PUT:key1:value1"), "Recovery should find 'PUT:key1:value1'");
        assertTrue(logEntries.contains("DELETE:key2"), "Recovery should find 'DELETE:key2'");
    }

    @Test
    void testLogOperationIsPersistent() throws IOException {
        wal.logOperation("PUT:key3:value3");

        // Simulate a restart by creating a new WriteAheadLog instance
        WriteAheadLog newWalInstance = new WriteAheadLog();

        assertTrue(newWalInstance.recoverLog().contains("PUT:key3:value3"),
                "Recovered log should contain the previously logged operation");
    }

    // Add more tests for edge cases, like empty log, etc.
}
