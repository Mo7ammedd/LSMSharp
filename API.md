# LSMSharp API Documentation

## Overview

LSMSharp is a high-performance LSM-Tree storage engine for .NET 8.0+ applications. This document describes the public API and usage patterns.

## Core Classes

### LSMTreeDB

The main entry point for interacting with the database.

#### Opening a Database

```csharp
// Open with default configuration
var db = await LSMTreeDB.OpenAsync("./mydb");

// Open with custom configuration
var config = new LSMConfiguration
{
    MemtableThreshold = 1024 * 1024,  // 1MB memtable
    DataBlockSize = 4096,              // 4KB blocks
    CompressionType = CompressionType.GZip,
    EnableBlockCache = true,
    BlockCacheSize = 64 * 1024 * 1024  // 64MB cache
};
var db = await LSMTreeDB.OpenAsync("./mydb", config);
```

#### Basic Operations

**Set (Insert/Update)**
```csharp
await db.SetAsync("key", Encoding.UTF8.GetBytes("value"));
```

**Get (Read)**
```csharp
var (found, value) = await db.GetAsync("key");
if (found)
{
    Console.WriteLine(Encoding.UTF8.GetString(value));
}
```

**Delete**
```csharp
await db.DeleteAsync("key");
```

**Range Scan** (New in v1.0)
```csharp
await foreach (var (key, value) in db.RangeAsync("start_key", "end_key"))
{
    Console.WriteLine($"{key} => {Encoding.UTF8.GetString(value)}");
}
```

#### Maintenance Operations

**Manual Flush**
```csharp
await db.FlushAsync();
```

**Manual Compaction**
```csharp
await db.CompactAsync();
```

#### Statistics and Monitoring

**Cache Statistics**
```csharp
var cacheStats = db.GetCacheStats();
if (cacheStats.HasValue)
{
    Console.WriteLine($"Cache Hit Ratio: {cacheStats.Value.HitRatio:P2}");
    Console.WriteLine($"Cache Hits: {cacheStats.Value.Hits}");
    Console.WriteLine($"Cache Misses: {cacheStats.Value.Misses}");
}
```

**Database Statistics** (New in v1.0)
```csharp
var dbStats = db.GetDatabaseStats();
Console.WriteLine($"Active Memtable Size: {dbStats.ActiveMemtableSize} bytes");
Console.WriteLine($"Flushing in Progress: {dbStats.IsFlushingInProgress}");
```

**Clear Cache**
```csharp
db.ClearCache();
```

#### Cleanup

```csharp
await db.DisposeAsync();
// or with using statement
await using var db = await LSMTreeDB.OpenAsync("./mydb");
```

## Configuration

### LSMConfiguration

Configuration options for the database.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `MemtableThreshold` | `int` | 1048576 (1MB) | Size threshold for flushing memtable to disk |
| `DataBlockSize` | `int` | 4096 (4KB) | Size of data blocks in SSTables |
| `BloomFilterFalsePositiveRate` | `double` | 0.01 (1%) | Target false positive rate for Bloom filters |
| `CompactionThreads` | `int` | 1 | Number of threads for compaction (future use) |
| `CompressionType` | `CompressionType` | `GZip` | Compression algorithm (None, GZip, LZ4) |
| `FlushInterval` | `TimeSpan` | 30 seconds | Background flush interval |
| `BlockCacheSize` | `long` | 67108864 (64MB) | Size of block cache |
| `EnableBlockCache` | `bool` | `true` | Enable/disable block caching |
| `MaxLevels` | `int` | 7 | Maximum number of levels |
| `Level0CompactionTrigger` | `int` | 4 | Number of L0 files to trigger compaction |
| `CompactionRatio` | `double` | 10.0 | Size ratio between levels |

## Performance Tuning

### Write-Heavy Workloads

```csharp
var config = new LSMConfiguration
{
    MemtableThreshold = 64 * 1024 * 1024,  // Larger memtable (64MB)
    BlockCacheSize = 128 * 1024 * 1024,    // Larger cache (128MB)
    CompressionType = CompressionType.LZ4   // Faster compression
};
```

### Read-Heavy Workloads

```csharp
var config = new LSMConfiguration
{
    BloomFilterFalsePositiveRate = 0.001,   // Lower FPR (0.1%)
    BlockCacheSize = 256 * 1024 * 1024,     // Larger cache (256MB)
    DataBlockSize = 32 * 1024               // Larger blocks (32KB)
};
```

### Space-Constrained Environments

```csharp
var config = new LSMConfiguration
{
    MemtableThreshold = 256 * 1024,         // Smaller memtable (256KB)
    BlockCacheSize = 16 * 1024 * 1024,      // Smaller cache (16MB)
    CompressionType = CompressionType.GZip  // Better compression
};
```

## Examples

### Example 1: Simple Key-Value Store

```csharp
await using var db = await LSMTreeDB.OpenAsync("./data");

// Store user data
await db.SetAsync("user:1", Encoding.UTF8.GetBytes("Alice"));
await db.SetAsync("user:2", Encoding.UTF8.GetBytes("Bob"));

// Retrieve user data
var (found, value) = await db.GetAsync("user:1");
Console.WriteLine(Encoding.UTF8.GetString(value)); // "Alice"
```

### Example 2: Range Query

```csharp
await using var db = await LSMTreeDB.OpenAsync("./data");

// Insert sequential data
for (int i = 1; i <= 100; i++)
{
    await db.SetAsync($"item:{i:D3}", Encoding.UTF8.GetBytes($"Value {i}"));
}

// Query a range
await foreach (var (key, value) in db.RangeAsync("item:050", "item:060"))
{
    Console.WriteLine($"{key} => {Encoding.UTF8.GetString(value)}");
}
```

### Example 3: Monitoring Performance

```csharp
var config = new LSMConfiguration
{
    EnableBlockCache = true,
    BlockCacheSize = 64 * 1024 * 1024
};

await using var db = await LSMTreeDB.OpenAsync("./data", config);

// Perform operations...
for (int i = 0; i < 10000; i++)
{
    await db.SetAsync($"key:{i}", Encoding.UTF8.GetBytes($"value:{i}"));
}

// Check statistics
var dbStats = db.GetDatabaseStats();
var cacheStats = db.GetCacheStats();

Console.WriteLine($"Memtable Size: {dbStats.TotalMemtableSize:N0} bytes");
Console.WriteLine($"Cache Hit Ratio: {cacheStats?.HitRatio:P2}");
```

## Thread Safety

All public methods of `LSMTreeDB` are thread-safe and can be called concurrently from multiple threads. The database uses fine-grained locking to ensure data consistency while maximizing concurrent throughput.

## Error Handling

The API throws the following exceptions:

- `ArgumentNullException`: When required parameters are null
- `ArgumentException`: When parameters have invalid values
- `ObjectDisposedException`: When operations are attempted on a disposed database
- `IOException`: When file I/O operations fail

Always use try-catch blocks or let exceptions propagate appropriately in your application.

## Best Practices

1. **Use `await using` for automatic cleanup**
   ```csharp
   await using var db = await LSMTreeDB.OpenAsync("./data");
   ```

2. **Batch writes when possible** - The database handles concurrent writes efficiently

3. **Monitor cache statistics** - Adjust `BlockCacheSize` based on hit ratio

4. **Use appropriate compression** - LZ4 for speed, GZip for space

5. **Periodic manual compaction** - For long-running applications with many updates/deletes

6. **Handle exceptions gracefully** - Especially for I/O operations

## Version History

### v1.0.0 (Current)
- Initial release
- Core CRUD operations
- Range scan support
- Bloom filters
- Block caching
- Write-ahead logging
- Leveled compaction
- XML documentation
- Performance monitoring APIs
