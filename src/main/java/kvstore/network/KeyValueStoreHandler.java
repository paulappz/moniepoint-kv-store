package kvstore.network;

import kvstore.KeyValueStore;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

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

                    // Handle only PUT method
                    if ("PUT".equals(method)) {
                        isPutRequest = true;
                    } else {
                        out.println("HTTP/1.1 405 Method Not Allowed");
                        return;
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

            // Handle PUT request
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

    // 1. Handle PUT (Single key-value Put)
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
