# LSM-Tree Storage Engine

A comprehensive implementation of an LSM-Tree (Log-Structured Merge-Tree) storage engine in C#.

## Overview

This project implements a complete LSM-Tree storage engine with the following components:

### Core Components

- **Skip List**: Thread-safe in-memory ordered data structure for the Memtable
- **WAL (Write-Ahead Log)**: Ensures durability and crash recovery
- **Memtable**: In-memory storage component using Skip List
- **SSTable**: Sorted String Table for persistent disk storage with compression
- **Bloom Filter**: Probabilistic data structure for efficient key existence checks
- **K-Way Merge**: Algorithm for merging multiple sorted sequences during compaction
- **Leveled Compaction**: Strategy for organizing SSTables across multiple levels

### Features

- **ACID Properties**: Atomicity and Durability through WAL
- **Crash Recovery**: Automatic recovery from WAL files on startup
- **Efficient Writes**: All writes go to memory first, then periodically flushed to disk
- **Optimized Reads**: Multi-level search with Bloom filters to minimize disk I/O
- **Background Compaction**: Automatic merging and cleanup of SSTables
- **Thread-Safe**: Concurrent reads and writes supported
- **Compression**: Data blocks are compressed using GZip for space efficiency

## Architecture

```
┌─────────────────┐    ┌─────────────────┐
│   Application   │    │      WAL        │
│                 │    │   (Durability)  │
└─────────┬───────┘    └─────────────────┘
          │                      │
          ▼                      │
┌─────────────────┐              │
│    Memtable     │◄─────────────┘
│   (Skip List)   │
└─────────┬───────┘
          │ Flush when full
          ▼
┌─────────────────┐
│   Level 0       │
│   SSTables      │
└─────────┬───────┘
          │ Compaction
          ▼
┌─────────────────┐
│   Level 1       │
│   SSTables      │
└─────────┬───────┘
          │ Compaction
          ▼
┌─────────────────┐
│   Level N       │
│   SSTables      │
└─────────────────┘
```

## Usage

### Basic Operations

```csharp
// Open or create a database
using var db = await LSMTreeDB.OpenAsync("./mydb");

// Set key-value pairs
await db.SetAsync("user:1", Encoding.UTF8.GetBytes("Alice"));
await db.SetAsync("user:2", Encoding.UTF8.GetBytes("Bob"));

// Get values
var (found, value) = await db.GetAsync("user:1");
if (found)
{
    Console.WriteLine($"Value: {Encoding.UTF8.GetString(value)}");
}

// Delete keys (using tombstones)
await db.DeleteAsync("user:2");

// Manual flush and compaction
await db.FlushAsync();
await db.CompactAsync();
```

### Configuration

```csharp
var db = await LSMTreeDB.OpenAsync(
    directory: "./mydb",
    memtableThreshold: 1024 * 1024,  // 1MB memtable threshold
    dataBlockSize: 4096               // 4KB data block size
);
```

## Implementation Details

### Write Path

1. **WAL Write**: Operation is first written to the Write-Ahead Log for durability
2. **Memtable Insert**: Entry is inserted into the in-memory Skip List
3. **Flush Check**: If memtable exceeds threshold, it's flushed to disk as SSTable
4. **Background Compaction**: SSTables are merged in background to maintain performance

### Read Path

1. **Active Memtable**: Search current active memtable first
2. **Flushing Memtable**: Search memtable being flushed (if any)
3. **SSTable Search**: Search SSTables from Level 0 to Level N
   - Use Bloom filters to skip SSTables that don't contain the key
   - Binary search within data blocks

### Compaction Strategy

- **Level 0**: SSTables can have overlapping key ranges
- **Level 1+**: SSTables within same level have non-overlapping key ranges
- **L0 to L1**: Select all overlapping SSTables from both levels
- **Ln to Ln+1**: Select one SSTable from Ln and all overlapping from Ln+1
- **K-Way Merge**: Merge selected SSTables, keeping newest values and removing tombstones

### Data Structures

#### Skip List
- Probabilistic data structure with O(log n) operations
- Multiple levels of linked lists for fast traversal
- Thread-safe implementation with proper locking

#### SSTable Format
```
[Data Blocks][Meta Block][Index Block][Footer]
```

- **Data Blocks**: Compressed sorted key-value pairs with prefix compression
- **Meta Block**: Metadata (creation time, level, key range)
- **Index Block**: Pointers to data blocks with key ranges
- **Footer**: Fixed-size block with pointers to meta and index blocks

#### Bloom Filter
- Configurable false positive rate
- Multiple hash functions (FNV-1a based)
- Serializable for storage in SSTables

## Performance Characteristics

- **Write Throughput**: ~10,000+ ops/sec (memory-bound)
- **Read Latency**: Sub-millisecond for hot data, few milliseconds for cold data
- **Space Amplification**: ~1.5x due to compaction overhead
- **Write Amplification**: ~10x in worst case (depends on compaction frequency)

## Building and Running

```bash
# Build the project
dotnet build

# Run the demo
dotnet run

# Run tests (if implemented)
dotnet test
```

## Project Structure

```
LSMTree/
├── Core/
│   ├── Interfaces.cs      # Core interfaces
│   └── Types.cs          # Basic data types
├── SkipList/
│   └── ConcurrentSkipList.cs  # Thread-safe skip list
├── WAL/
│   └── WriteAheadLog.cs  # Write-ahead logging
├── Memtable/
│   └── Memtable.cs       # In-memory storage
├── SSTable/
│   ├── SSTableBlocks.cs  # SSTable block implementations
│   └── SSTable.cs        # SSTable management
├── BloomFilter/
│   └── BloomFilter.cs    # Bloom filter implementation
├── Compaction/
│   └── LevelManager.cs   # Leveled compaction strategy
├── Utils/
│   └── KWayMerge.cs      # K-way merge algorithm
├── LSMTreeDB.cs          # Main database class
└── Program.cs            # Demo application
```

## Design Decisions

1. **Thread Safety**: Used locks judiciously to ensure correctness while maintaining performance
2. **Memory Management**: Careful attention to memory usage and cleanup
3. **Error Handling**: Graceful handling of I/O errors and corruption
4. **Async/Await**: Async operations for I/O-bound tasks
5. **Compression**: GZip compression for data blocks to reduce storage footprint
6. **Configurable**: Key parameters (thresholds, block sizes) are configurable

## Future Enhancements

- [ ] Snapshots and point-in-time recovery
- [ ] Range queries and iterators
- [ ] Multiple column families
- [ ] Better compression algorithms (LZ4, Snappy)
- [ ] Metrics and monitoring
- [ ] Write batching for better throughput
- [ ] Read caching
- [ ] Partitioning for horizontal scaling

## License

This project is for educational purposes and demonstrates the implementation of LSM-Tree concepts.
