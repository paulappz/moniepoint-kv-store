package kvstore.network;

import kvstore.KeyValueStore;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;
import java.util.stream.Collectors;


public class KeyValueStoreHandler implements Runnable {
    private Socket clientSocket;
    private KeyValueStore store;

    public KeyValueStoreHandler(Socket clientSocket, KeyValueStore store) {
        this.clientSocket = clientSocket;
        this.store = store;
    }

    @Override
    public void run() {
        System.out.println("Handling new client connection");

        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)
        ) {
            String inputLine;
            boolean isPostRequest = false;
            boolean isPutRequest = false;
            int contentLength = 0;

            // Read request line
            if ((inputLine = in.readLine()) != null) {
                System.out.println("Received request line: " + inputLine);

                String[] requestParts = inputLine.split(" ");
                if (requestParts.length >= 3) {
                    String method = requestParts[0].toUpperCase();
                    String path = requestParts[1];
                    String httpVersion = requestParts[2];

                    if (!httpVersion.startsWith("HTTP/")) {
                        out.println("HTTP/1.1 400 Bad Request");
                        return;
                    }

                    // Handle based on HTTP method
                    switch (method) {
                        case "GET" -> handleGetRequest(path, out);
                        case "POST" -> isPostRequest = true;
                        case "PUT" -> isPutRequest = true;
                        default -> {
                            out.println("HTTP/1.1 405 Method Not Allowed");
                            return;
                        }
                    }
                }
            }

            // Read headers to determine content length
            while (!(inputLine = in.readLine()).isEmpty()) {
                System.out.println("Header: " + inputLine);
                if (inputLine.startsWith("Content-Length:")) {
                    contentLength = Integer.parseInt(inputLine.split(":")[1].trim());
                }
            }

            // If it's a POST request, handle it
            if (isPostRequest) {
                if (contentLength > 0) {
                    char[] bodyChars = new char[contentLength];
                    int readChars = in.read(bodyChars, 0, contentLength);
                    if (readChars != contentLength) {
                        System.err.println("Warning: Expected to read " + contentLength + " characters but read " + readChars);
                    }
                    String requestBody = new String(bodyChars);
                    System.out.println("Received POST body: " + requestBody);
                    handlePostRequest(requestBody, out);
                } else {
                    out.println("HTTP/1.1 400 Bad Request");
                    out.println("Content-Type: text/plain");
                    out.println();
                    out.println("ERROR: Missing POST body");
                }
            }

            // If it's a PUT request, handle it separately
            if (isPutRequest) {
                if (contentLength > 0) {
                    char[] bodyChars = new char[contentLength];
                    int readChars = in.read(bodyChars, 0, contentLength);
                    if (readChars != contentLength) {
                        System.err.println("Warning: Expected to read " + contentLength + " characters but read " + readChars);
                    }
                    String requestBody = new String(bodyChars);
                    System.out.println("Received PUT body: " + requestBody);
                    handlePutRequest(requestBody, out);
                } else {
                    out.println("HTTP/1.1 400 Bad Request");
                    out.println("Content-Type: text/plain");
                    out.println();
                    out.println("ERROR: Missing PUT body");
                }
            }

        } catch (IOException e) {
            System.err.println("Error handling client connection: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing socket: " + e.getMessage());
            }
        }
    }

    // 1. Handle Get (Read)
    private void handleGetRequest(String path, PrintWriter out) {
        try {
            if (path.contains(",")) { // For range query
                handleRangeQuery(path, out);
                return;
            }

            String key = path.substring(1); // Assume path is like "/key"
            String value = store.get(key); // This may throw an exception if the key is not found

            if (value != null) {
                out.println("HTTP/1.1 200 OK");
                out.println("Content-Type: text/plain");
                out.println();
                out.println("VALUE: " + value);
            } else {
                out.println("HTTP/1.1 404 Not Found");
                out.println("Content-Type: text/plain");
                out.println();
                out.println("ERROR: Key not found");
            }
        } catch (NoSuchElementException e) {
            out.println("HTTP/1.1 404 Not Found");
            out.println("Content-Type: text/plain");
            out.println();
            out.println(e.getMessage()); // Return the custom error message from the exception
        } catch (IOException e) {
            out.println("HTTP/1.1 500 Internal Server Error");
            out.println("Content-Type: text/plain");
            out.println();
            out.println("ERROR: An error occurred while processing the request");
        } finally {
            // Ensure the data is sent before closing the connection
            out.flush();
        }
    }

    // 2. Handle Range Query (ReadKeyRange)
    private void handleRangeQuery(String path, PrintWriter out) throws IOException {
        String[] keys = path.substring(1).split(","); // Assume path is like "/startKey,endKey"

        if (keys.length == 2) {
            String startKey = keys[0].trim();
            String endKey = keys[1].trim();

            if (startKey.isEmpty() || endKey.isEmpty()) {
                out.println("HTTP/1.1 400 Bad Request");
                out.println("Content-Type: text/plain");
                out.println();
                out.println("ERROR: Invalid range request, keys cannot be empty");
                return;
            }

            // Retrieve the range result
            List<String[]> rangeResult = store.readKeyRange(startKey, endKey);

            // Debug log to check retrieved results
            System.out.println("Range Query from " + startKey + " to " + endKey + ": " + rangeResult);

            if (rangeResult == null || rangeResult.isEmpty()) {
                out.println("HTTP/1.1 204 No Content");
                out.println("Content-Type: text/plain");
                out.println();
                out.println("RANGE RESULT: No values found in the specified range.");
            } else {
                // Join the results for display
                String joinedResult = rangeResult.stream()
                        .map(pair -> pair[0] + "=" + pair[1])
                        .collect(Collectors.joining(", "));
                out.println("HTTP/1.1 200 OK");
                out.println("Content-Type: text/plain");
                out.println();
                out.println("RANGE VALUES: " + joinedResult);
            }
        } else {
            out.println("HTTP/1.1 400 Bad Request");
            out.println("Content-Type: text/plain");
            out.println();
            out.println("ERROR: Invalid range request format, expected /startKey,endKey");
        }
    }

    // 3. Handle POST (Put)
    private void handlePostRequest(String requestBody, PrintWriter out) throws IOException {
        String[] keyValue = requestBody.split("&");
        String key = null;
        String value = null;

        for (String pair : keyValue) {
            String[] kv = pair.split("=");
            if (kv.length == 2) {
                if ("key".equals(kv[0].trim())) {
                    key = kv[1].trim();
                } else if ("value".equals(kv[0].trim())) {
                    value = kv[1].trim();
                }
            }
        }

        if (key != null && value != null) {
            store.put(key, value);
            out.println("HTTP/1.1 200 OK");
            out.println("Content-Type: text/plain");
            out.println();
            out.println("OK: Key stored");
        } else {
            out.println("HTTP/1.1 400 Bad Request");
            out.println("Content-Type: text/plain");
            out.println();
            out.println("ERROR: Invalid parameters");
        }
    }

    // 4. Handle PUT (Single key-value Put)
    private void handlePutRequest(String requestBody, PrintWriter out) throws IOException {
        String[] keyValue = requestBody.split("&");
        String key = null;
        String value = null;

        for (String pair : keyValue) {
            String[] kv = pair.split("=");
            if (kv.length == 2) {
                if ("key".equals(kv[0].trim())) {
                    key = kv[1].trim();
                } else if ("value".equals(kv[0].trim())) {
                    value = kv[1].trim();
                }
            }
        }

        if (key != null && value != null) {
            store.put(key, value);
            out.println("HTTP/1.1 200 OK");
            out.println("Content-Type: text/plain");
            out.println();
            out.println("OK: Key stored");
        } else {
            out.println("HTTP/1.1 400 Bad Request");
            out.println("Content-Type: text/plain");
            out.println();
            out.println("ERROR: Invalid parameters");
        }
    }
}
