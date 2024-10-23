package kvstore.network;

import kvstore.KeyValueStore;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KeyValueStoreServer {
    private static final int DEFAULT_PORT = 8081;
    private static final int TIMEOUT_MS = 30000;  // 30 seconds socket timeout
    private KeyValueStore store;
    private ExecutorService executor;
    private ServerSocket serverSocket;


    public KeyValueStoreServer(int port) throws IOException {
        store = new KeyValueStore();
        executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());  // Dynamically adjust pool size
        serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(TIMEOUT_MS);  // Set socket timeout

    }

    public void start() {
        System.out.println("Server listening on port " + serverSocket.getLocalPort());

        try {
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    clientSocket.setSoTimeout(TIMEOUT_MS);  // Set timeout on individual sockets

                    // Pass the required arguments: Socket, KeyValueStore, NetworkManager, HeartbeatManager
                    executor.submit(new KeyValueStoreHandler(clientSocket, store /*, networkManager, heartbeatManager*/));
                } catch (IOException e) {
                    System.err.println("Error accepting client connection: " + e.getMessage());
                }
            }
        } finally {
            shutdown();
        }
    }

    // Gracefully shutdown the server and executor service
    public void shutdown() {
        try {
            System.out.println("Shutting down server...");
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
            executor.shutdown();
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                System.err.println("Executor did not terminate in the specified time.");
                executor.shutdownNow();
            }
            System.out.println("Server shutdown complete.");
        } catch (IOException | InterruptedException e) {
            System.err.println("Error shutting down the server: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;

        try {
            KeyValueStoreServer server = new KeyValueStoreServer(port);
            server.start();
        } catch (IOException e) {
            System.err.println("Failed to start the server: " + e.getMessage());
        }
    }
}
