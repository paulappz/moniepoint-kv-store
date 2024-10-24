package kvstore;

public class Node {
    private String address;
    private KeyValueStore store;

    public Node(String address, KeyValueStore store) {
        this.address = address;
        this.store = store;
    }

    public String getAddress() {
        return address;
    }

    public KeyValueStore getStore() {
        return store;
    }

    public boolean isActive() {
        return store.isActive();
    }
}
