# Key-Value Store Project

## Overview
This project implements a persistent Key/Value storage system that is designed to be network-available. It follows a pluggable architecture with components that ensure low latency, high throughput, and crash resilience.

## Features
- **Put(Key, Value)**: Store a key-value pair in the system.
- **Read(Key)**: Retrieve the value associated with a key.
- **ReadKeyRange(StartKey, EndKey)**: Retrieve key-value pairs within a specified range.
- **BatchPut(..keys, ..values)**: Insert multiple key-value pairs in a single operation.
- **Delete(key)**: Remove a key and its associated value.

## Bonus Features
1. Data replication across multiple nodes.
2. Automatic failover handling for high availability.

## Design Objectives

This implementation aims to achieve the following objectives:

1. **Low Latency**: Fast operations for reading and writing data.
2. **High Throughput**: Efficient handling of a large number of write operations.
3. **Memory Management**: Capable of managing datasets larger than RAM without performance degradation.
4. **Crash Friendliness**: Quick recovery from crashes and ensuring no data loss.
5. **Predictable Behavior**: Consistent performance under heavy load and large volumes of data.

## Assumptions
- The system is designed to be run in a distributed environment, supporting multiple nodes for data replication.
- Each node in the system can independently process requests and maintain its own data store.
- The `WriteAheadLog` (WAL) is implemented to ensure durability and crash recovery.
- The LSM Tree (Log-Structured Merge-tree) is used for efficient write operations and to manage large datasets.
- The `LRUCache` (Least Recently Used Cache) is implemented for quick access to frequently used data.
- All operations are performed in a thread-safe manner to handle concurrent requests.
- The application has basic error handling for operations like `put`, `get`, and `delete`.
- The system can handle a high throughput of write operations due to the design choices made.

## Replication and Failover

### Replication
Data is replicated across multiple nodes to ensure redundancy and high availability. When a `Put` or `Delete` operation is performed, the changes are propagated to all active nodes in the cluster.

### Automatic Failover
The system can detect node failures and automatically reroute requests to available nodes. Upon recovery, a failed node synchronizes its state with the primary node to ensure consistency.


## Key-Value Store Operations

![Architecture Diagram](<architecture.webp>)


## Getting Started
### Prerequisites
- Java JDK (version 8 or higher)
- Maven (for building the project)

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/paulappz/moniepoint-kv-store.git
   cd moniepoint-kv-store
   ```
2. Build the project using Maven:
   ```bash
   mvn clean install
   ```
3. Run the project using Maven:
   ```bash
   mvn exec:java
   ```

4. To run tests
   ```bash
   mvn test
   ```

5 Moniepoint Key-Value Store Directory Structure

```
moniepoint-kv-store/
├── src/                                      # Source code directory
│   ├── main/                                 # Main source directory
│   │   ├── java/                             # Java source files
│   │   │   ├── kvstore/                      # Package for key-value store implementation
│   │   │   │   ├── KeyValueStore.java        # Class for managing key-value pairs
│   │   │   │   ├── LRUCache.java             # Class implementing Least Recently Used Cache
│   │   │   │   ├── LSMTree.java              # Class implementing Log-Structured Merge Tree
│   │   │   │   ├── Node.java                 # Class representing a node in the LSM Tree
│   │   │   │   ├── SSTable.java              # Class for managing SSTables
│   │   │   │   ├── WriteAheadLog.java        # Class for the Write Ahead Log mechanism
│   │   │   │   ├── network/                  # Package for network-related components
│   │   │   │   │   ├── KeyValueStoreHandler.java # Class handling requests to the key-value store
│   │   │   │   │   ├── KeyValueStoreServer.java  # Class for running the key-value store server
│   │   │   │   └── Main.java                 # Main entry point of the application
│   ├── test/                                 # Test source directory
│   │   ├── java/                             # Java test files
│   │   │   ├── com/                          # Base package for tests
│   │   │   │   ├── kvstore/                  # Package for tests related to key-value store
│   │   │   │   │   ├── KeyValueStoreTest.java # Tests for KeyValueStore functionality
│   │   │   │   │   ├── LRUCacheTest.java     # Tests for LRUCache functionality
│   │   │   │   │   ├── WriteAheadLogTest.java # Tests for WriteAheadLog functionality (if implemented)
├── target/  
│   ├── pom.xml                                # Maven project file
```


### API Interfaces

The following operations are available for interacting with the Key-Value Store:


**Put(Key, Value)**

-  Inserts or updates the specified key with the provided value.
- Example: `put("key1", "value1");`
- CURL Example: `curl -X PUT http://localhost:8081/ -d "key=user1&value=Ayo"`
- CURL Response: `OK: Key stored`

**Read(Key)**

- Retrieves the value associated with the specified key.
- Example: `String value = read("key1");`
- CURL Example: `curl -X GET http://localhost:8081/user1`
- CURL Response: `VALUE: Alice`

**ReadKeyRange(StartKey, EndKey)**

- Retrieves all key-value pairs within the specified range.
- Example: `List<String[]> results = readKeyRange("key1", "key3");`
- CURL Example: `curl -X GET http://localhost:8081/user1,user3`
- CURL Response: `RANGE VALUES: user1=Alice, user3=Ayo`
- CURL Example (when no values are found): `curl -X GET http://localhost:8081/user1,user2`
- CURL Response: `ERROR: No values found in the specified range.`

**BatchPut(..keys, ..values)**

- Inserts multiple key-value pairs in a single operation.
- Example: `batchPut(List.of("key1", "key2"), List.of("value1", "value2"));`
- CURL Example: `curl -X POST http://localhost:8081/ -d "key1=user1&value1=Alice&key2=user2&value2=Bob"`
- CURL Response: `OK: Keys stored`

**Delete(key)**

- Removes the specified key from the store.
- Example: `delete("key1");`
- CURL Example: `curl -X DELETE http://localhost:8081/user1`
- CURL Response: `OK: Key deleted`
- CURL Example (Deleting a non-existing key): `curl -X DELETE http://localhost:8081/user1`
- CURL Response: `ERROR: Key not found or already deleted`
- CURL Example: `curl -X DELETE http://localhost:8081/user2`
- CURL Response: `OK: Key deleted`
- CURL Example (Deleting a non-existing key again): `curl -X DELETE http://localhost:8081/user2`
- CURL Response: `ERROR: Key not found or already deleted`


##Trade-offs Made to Achieve Objectives

 **Performance vs. Consistency:**
In a distributed system, prioritizing high availability and low latency might compromise strong consistency. This implementation employs eventual consistency for replication, ensuring that data is quickly accessible on each active node, even if some nodes haven’t fully synchronized. This supports low-latency and high-throughput objectives but could result in brief periods of inconsistency between nodes.


**Memory vs. Disk Persistence:**
To handle large datasets exceeding the available RAM, this system leverages Log-Structured Merge (LSM) trees. LSM trees minimize in-memory data storage by using an on-disk structure for efficient reads and writes. While this enables handling larger-than-RAM datasets, it can incur read penalties as data is merged from multiple levels on disk, impacting read latency.

**Write Throughput vs. Recovery Speed:**
Write-Ahead Logging (WAL) ensures data persistence before changes reach the main data structure. WAL enables quick recovery from crashes by replaying logs, which is essential for crash friendliness. However, this slightly slows down write throughput due to the overhead of logging each operation.

**Replication Overhead vs. Fault Tolerance:**
Replicating data to multiple nodes increases fault tolerance, but incurs additional network latency and storage requirements, impacting throughput. This system adopts asynchronous replication to balance fault tolerance and write performance, allowing nodes to continue serving requests during the replication process without waiting for all nodes to sync.

**Cache Size vs. Memory Constraints:**
An LRU (Least Recently Used) cache accelerates frequently accessed data, but a larger cache consumes more memory, potentially impacting other parts of the application. This design uses a moderately-sized cache to improve read speeds without overcommitting RAM, balancing memory usage with read performance.

**Batch Processing vs. Real-Time Responsiveness:**
Batch operations, such as BatchPut, offer high throughput by reducing per-operation overhead. However, processing large batches can delay real-time responses in concurrent scenarios. This system prioritizes throughput in batch processing while ensuring the ability to handle real-time single operations efficiently.
Each trade-off was carefully chosen to ensure the system meets Moniepoint’s key objectives for durability, performance, scalability, and resilience under heavy load conditions, while remaining flexible to handle evolving requirements.


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
