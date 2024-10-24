package kvstore.network;

import kvstore.KeyValueStore;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KeyValueStoreHandler implements Runnable {
    private final Socket clientSocket;
    private final KeyValueStore store;

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
                handleRequestBody(in, contentLength, out, this::handlePostRequest);
            }

            // If it's a PUT request, handle it separately
            if (isPutRequest) {
                handleRequestBody(in, contentLength, out, this::handlePutRequest);
            }

        } catch (IOException e) {
            System.err.println("Error handling client connection: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing socket: " + e.getMessage());
            }
        }
    }

    // Common method to handle request bodies for POST and PUT
    private void handleRequestBody(BufferedReader in, int contentLength, PrintWriter out, RequestHandler handler) throws IOException {
        if (contentLength > 0) {
            char[] bodyChars = new char[contentLength];
            int readChars = in.read(bodyChars, 0, contentLength);
            if (readChars != contentLength) {
                System.err.println("Warning: Expected to read " + contentLength + " characters but read " + readChars);
            }
            String requestBody = new String(bodyChars);
            System.out.println("Received body: " + requestBody);
            handler.handle(requestBody, out);
        } else {
            out.println("HTTP/1.1 400 Bad Request");
            out.println("Content-Type: text/plain");
            out.println();
            out.println("ERROR: Missing request body");
        }
    }

    // Functional interface for request handling
    @FunctionalInterface
    interface RequestHandler {
        void handle(String requestBody, PrintWriter out) throws IOException;
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

            if (rangeResult.isEmpty()) {
                out.println("HTTP/1.1 404 Not Found");
                out.println("Content-Type: text/plain");
                out.println();
                out.println("ERROR: No keys found in the specified range");
            } else {
                out.println("HTTP/1.1 200 OK");
                out.println("Content-Type: text/plain");
                out.println();
                for (String[] entry : rangeResult) {
                    out.println(entry[0] + "=" + entry[1]);
                }
            }
        } else {
            out.println("HTTP/1.1 400 Bad Request");
            out.println("Content-Type: text/plain");
            out.println();
            out.println("ERROR: Invalid range request format");
        }
    }

    // 3. Handle Post (BatchPut)
    private void handlePostRequest(String requestBody, PrintWriter out) throws IOException {
        // Decode the URL-encoded request body
        String decodedBody = java.net.URLDecoder.decode(requestBody, "UTF-8");
        try {
            String[] pairs = decodedBody.split("&");
            Map<String, String> keyValuePairs = new HashMap<>();

            for (String pair : pairs) {
                String[] keyValue = pair.split("=");
                if (keyValue.length == 2) {
                    keyValuePairs.put(keyValue[0].trim(), keyValue[1].trim());
                } else {
                    out.println("HTTP/1.1 400 Bad Request");
                    out.println("Content-Type: text/plain");
                    out.println();
                    out.println("ERROR: Invalid key-value pair format in request body");
                    return;
                }
            }

            store.batchPut(keyValuePairs);
            out.println("HTTP/1.1 200 OK");
            out.println("Content-Type: text/plain");
            out.println();
            out.println("Successfully added/updated keys: " + keyValuePairs.keySet());

        } catch (IllegalArgumentException e) {
            out.println("HTTP/1.1 400 Bad Request");
            out.println("Content-Type: text/plain");
            out.println();
            out.println("ERROR: " + e.getMessage());
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

    // 4. Handle Put (Single Put)
    private void handlePutRequest(String requestBody, PrintWriter out) throws IOException {
        try {
            String[] keyValue = requestBody.split("=");
            if (keyValue.length == 2) {
                String key = keyValue[0].trim();
                String value = keyValue[1].trim();
                store.put(key, value);
                out.println("HTTP/1.1 200 OK");
                out.println("Content-Type: text/plain");
                out.println();
                out.println("Successfully inserted/updated key: " + key);
            } else {
                out.println("HTTP/1.1 400 Bad Request");
                out.println("Content-Type: text/plain");
                out.println();
                out.println("ERROR: Invalid key-value pair format in request body");
            }
        } catch (IllegalArgumentException e) {
            out.println("HTTP/1.1 400 Bad Request");
            out.println("Content-Type: text/plain");
            out.println();
            out.println("ERROR: " + e.getMessage());
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
}
