using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LSMTree.Core;
using LSMTree.Memtable;
using LSMTree.SSTable;
using LSMTree.Compaction;

namespace LSMTree
{
    /// <summary>
    /// The main LSM-Tree storage engine implementation providing ACID guarantees and concurrent access.
    /// </summary>
    /// <remarks>
    /// This class implements a Log-Structured Merge-Tree database with the following features:
    /// <list type="bullet">
    /// <item>Write-ahead logging for durability</item>
    /// <item>In-memory memtables with automatic flushing</item>
    /// <item>Leveled compaction strategy</item>
    /// <item>Bloom filters for efficient key lookups</item>
    /// <item>Block-based compression</item>
    /// <item>Concurrent read/write support</item>
    /// </list>
    /// </remarks>
    public class LSMTreeDB : ILSMTree, IAsyncDisposable
    {
        private readonly string _directory;
        private readonly LSMConfiguration _config;
        private readonly LevelManager _levelManager;
        private readonly SemaphoreSlim _flushSemaphore;
        private readonly object _memtableLock = new object();
        private readonly IBlockCache? _blockCache;

        private IMemtable _activeMemtable;
        private IMemtable? _flushingMemtable;
        private long _nextWalId;
        private bool _disposed = false;

        public LSMTreeDB(string directory, LSMConfiguration? config = null)
        {
            _directory = directory ?? throw new ArgumentNullException(nameof(directory));
            _config = config ?? LSMConfiguration.Default;
            _config.Validate();

            if (!Directory.Exists(_directory))
            {
                Directory.CreateDirectory(_directory);
            }

            _blockCache = _config.EnableBlockCache ? new LRUBlockCache(_config.BlockCacheSize) : null;
            _levelManager = new LevelManager(
                Path.Combine(_directory, "levels"), 
                _blockCache, 
                _config.Level0CompactionTrigger, 
                (int)_config.CompactionRatio, 
                _config.DataBlockSize, 
                _config.CompressionType);
            _flushSemaphore = new SemaphoreSlim(1, 1);
            _nextWalId = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            _activeMemtable = CreateNewMemtable();
        }

        /// <summary>
        /// Opens or creates an LSM-Tree database at the specified directory.
        /// </summary>
        /// <param name="directory">The directory path where database files will be stored.</param>
        /// <param name="config">Optional configuration settings. If null, default configuration is used.</param>
        /// <returns>A task that returns an opened LSMTreeDB instance.</returns>
        /// <exception cref="ArgumentNullException">Thrown when directory is null.</exception>
        public static async Task<LSMTreeDB> OpenAsync(
            string directory, 
            LSMConfiguration? config = null)
        {
            var db = new LSMTreeDB(directory, config);
            await db.RecoverAsync();
            return db;
        }

        /// <summary>
        /// Asynchronously sets a key-value pair in the database.
        /// </summary>
        /// <param name="key">The key to set. Must not be null or empty.</param>
        /// <param name="value">The value to associate with the key.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentException">Thrown when key is null or empty.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the database has been disposed.</exception>
        public async Task SetAsync(string key, byte[] value)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(LSMTreeDB));

            if (string.IsNullOrEmpty(key))
                throw new ArgumentException("Key cannot be null or empty", nameof(key));

            var entry = new Entry(key, value ?? Array.Empty<byte>());
            
            IMemtable memtableToUse;
            lock (_memtableLock)
            {
                if (_disposed)
                    throw new ObjectDisposedException(nameof(LSMTreeDB));
                memtableToUse = _activeMemtable;
            }

            try
            {
                await memtableToUse.SetAsync(entry);

                if (memtableToUse.ShouldFlush(_config.MemtableThreshold))
                {
                    await TriggerFlushAsync();
                }
            }
            catch (ObjectDisposedException)
            {
                // If memtable was disposed, check if we're shutting down
                if (_disposed)
                    throw new ObjectDisposedException(nameof(LSMTreeDB));
                throw;
            }
        }

        /// <summary>
        /// Asynchronously retrieves the value associated with the specified key.
        /// </summary>
        /// <param name="key">The key to retrieve.</param>
        /// <returns>A task containing a tuple with a boolean indicating if the key was found and the associated value.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the database has been disposed.</exception>
        public async Task<(bool found, byte[] value)> GetAsync(string key)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(LSMTreeDB));

            if (string.IsNullOrEmpty(key))
                return (false, Array.Empty<byte>());

            // Search in active memtable first
            IMemtable activeMemtable;
            IMemtable? flushingMemtable;
            
            lock (_memtableLock)
            {
                activeMemtable = _activeMemtable;
                flushingMemtable = _flushingMemtable;
            }

            var (found, entry) = activeMemtable.Get(key);
            if (found)
            {
                return entry.Tombstone ? (false, Array.Empty<byte>()) : (true, entry.Value);
            }

            // Search in flushing memtable if it exists
            if (flushingMemtable != null)
            {
                (found, entry) = flushingMemtable.Get(key);
                if (found)
                {
                    return entry.Tombstone ? (false, Array.Empty<byte>()) : (true, entry.Value);
                }
            }

            // Search in SSTables
            var result = await _levelManager.SearchAsync(key);
            if (result.found)
            {
                return result.entry.Tombstone ? (false, Array.Empty<byte>()) : (true, result.entry.Value);
            }

            return (false, Array.Empty<byte>());
        }

        /// <summary>
        /// Asynchronously deletes a key from the database by writing a tombstone marker.
        /// </summary>
        /// <param name="key">The key to delete. Must not be null or empty.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentException">Thrown when key is null or empty.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the database has been disposed.</exception>
        public async Task DeleteAsync(string key)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(LSMTreeDB));

            if (string.IsNullOrEmpty(key))
                throw new ArgumentException("Key cannot be null or empty", nameof(key));

            // Create tombstone entry
            var entry = new Entry(key, Array.Empty<byte>(), tombstone: true);
            
            IMemtable memtableToUse;
            lock (_memtableLock)
            {
                memtableToUse = _activeMemtable;
            }

            await memtableToUse.SetAsync(entry);

            if (memtableToUse.ShouldFlush(_config.MemtableThreshold))
            {
                await TriggerFlushAsync();
            }
        }

        /// <summary>
        /// Asynchronously flushes the active memtable to disk as an SSTable.
        /// </summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the database has been disposed.</exception>
        public async Task FlushAsync()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(LSMTreeDB));

            await _flushSemaphore.WaitAsync();
            try
            {
                await FlushMemtableAsync();
            }
            finally
            {
                _flushSemaphore.Release();
            }
        }

        /// <summary>
        /// Asynchronously triggers compaction of SSTables to merge and eliminate obsolete data.
        /// </summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the database has been disposed.</exception>
        public Task CompactAsync()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(LSMTreeDB));

            return _levelManager.CompactAsync(0);
        }

        /// <summary>
        /// Asynchronously performs a range scan between the specified start and end keys (inclusive).
        /// </summary>
        /// <param name="startKey">The starting key of the range (inclusive). Must not be null or empty.</param>
        /// <param name="endKey">The ending key of the range (inclusive). Must not be null or empty.</param>
        /// <returns>An async enumerable of key-value pairs within the range, sorted by key.</returns>
        /// <exception cref="ArgumentException">Thrown when startKey or endKey is null/empty, or when startKey > endKey.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the database has been disposed.</exception>
        public async IAsyncEnumerable<(string key, byte[] value)> RangeAsync(string startKey, string endKey)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(LSMTreeDB));

            if (string.IsNullOrEmpty(startKey))
                throw new ArgumentException("Start key cannot be null or empty", nameof(startKey));

            if (string.IsNullOrEmpty(endKey))
                throw new ArgumentException("End key cannot be null or empty", nameof(endKey));

            if (string.CompareOrdinal(startKey, endKey) > 0)
                throw new ArgumentException("Start key must be less than or equal to end key");

            // Collect entries from all sources
            var allEntries = new Dictionary<string, Entry>();

            // Get snapshots of memtables
            IMemtable activeMemtable;
            IMemtable? flushingMemtable;
            
            lock (_memtableLock)
            {
                activeMemtable = _activeMemtable;
                flushingMemtable = _flushingMemtable;
            }

            // Collect from active memtable
            foreach (var entry in activeMemtable.GetAll())
            {
                if (string.CompareOrdinal(entry.Key, startKey) >= 0 && 
                    string.CompareOrdinal(entry.Key, endKey) <= 0)
                {
                    allEntries[entry.Key] = entry;
                }
            }

            // Collect from flushing memtable
            if (flushingMemtable != null)
            {
                foreach (var entry in flushingMemtable.GetAll())
                {
                    if (string.CompareOrdinal(entry.Key, startKey) >= 0 && 
                        string.CompareOrdinal(entry.Key, endKey) <= 0)
                    {
                        if (!allEntries.ContainsKey(entry.Key) || entry.Timestamp > allEntries[entry.Key].Timestamp)
                        {
                            allEntries[entry.Key] = entry;
                        }
                    }
                }
            }

            // Collect from SSTables through level manager
            var sstableEntries = await _levelManager.RangeScanAsync(startKey, endKey);
            foreach (var entry in sstableEntries)
            {
                if (!allEntries.ContainsKey(entry.Key) || entry.Timestamp > allEntries[entry.Key].Timestamp)
                {
                    allEntries[entry.Key] = entry;
                }
            }

            // Return sorted, non-tombstone entries
            foreach (var kvp in allEntries.OrderBy(e => e.Key))
            {
                if (!kvp.Value.Tombstone)
                {
                    yield return (kvp.Key, kvp.Value.Value);
                }
            }
        }

        private Task TriggerFlushAsync()
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    await FlushAsync();
                }
                catch
                {
                }
            });
            return Task.CompletedTask;
        }

        private async Task FlushMemtableAsync()
        {
            IMemtable memtableToFlush;
            
            lock (_memtableLock)
            {
                if (_activeMemtable.Size == 0)
                    return;

                _activeMemtable.MakeReadOnly();
                _flushingMemtable = _activeMemtable;
                _activeMemtable = CreateNewMemtable();
            }

            memtableToFlush = _flushingMemtable!;

            try
            {
                if (memtableToFlush is Memtable.Memtable mt)
                {
                    await mt.SyncWalAsync();
                }

                var entries = memtableToFlush.GetAll();

                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                var sstableFile = Path.Combine(_directory, "levels", $"L0_{timestamp}.sst");
                
                await SSTable.SSTable.BuildAsync(sstableFile, entries, 0, _config.DataBlockSize, _config.CompressionType);

                await _levelManager.AddSSTableAsync(sstableFile);

                if (memtableToFlush is Memtable.Memtable memtable)
                {
                    await memtable.DeleteWalAsync();
                }
            }
            finally
            {
                // Clear flushing memtable reference
                lock (_memtableLock)
                {
                    _flushingMemtable = null;
                }

                memtableToFlush.Dispose();
            }
        }

        private IMemtable CreateNewMemtable()
        {
            var walFile = Path.Combine(_directory, $"wal_{_nextWalId++}.wal");
            return new Memtable.Memtable(walFile);
        }

        private async Task RecoverAsync()
        {
            // Recover from existing WAL files
            var walFiles = Directory.GetFiles(_directory, "*.wal");
            
            foreach (var walFile in walFiles)
            {
                try
                {
                    var memtable = await MemtableFactory.CreateAsync(walFile, recover: true);
                    
                    if (memtable.Size > 0)
                    {
                        // Flush recovered memtable to SSTable
                        var entries = memtable.GetAll();
                        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                        var sstableFile = Path.Combine(_directory, "levels", $"L0_recovered_{timestamp}.sst");
                        
                        await SSTable.SSTable.BuildAsync(sstableFile, entries, 0, _config.DataBlockSize, _config.CompressionType);
                        await _levelManager.AddSSTableAsync(sstableFile);
                    }

                    // Clean up WAL file
                    await memtable.DeleteWalAsync();
                    memtable.Dispose();
                }
                catch
                {
                    // Skip corrupted WAL files
                }
            }

            // Load existing SSTables
            var levelsDir = Path.Combine(_directory, "levels");
            if (Directory.Exists(levelsDir))
            {
                var sstFiles = Directory.GetFiles(levelsDir, "*.sst");
                foreach (var sstFile in sstFiles)
                {
                    try
                    {
                        await _levelManager.AddSSTableAsync(sstFile);
                    }
                    catch
                    {
                        // Skip corrupted SSTable files
                    }
                }
            }
        }

        public void Dispose()
        {
            DisposeAsync().AsTask().Wait(TimeSpan.FromSeconds(30));
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            // Wait for any ongoing operations to complete
            await _flushSemaphore.WaitAsync();
            
            try
            {
                _disposed = true;

                // Flush any remaining data
                try
                {
                    await FlushMemtableAsync();
                }
                catch
                {
                    // Ignore flush errors during disposal
                }

                _activeMemtable?.Dispose();
                _flushingMemtable?.Dispose();
                _levelManager?.Dispose();
            }
            finally
            {
                _flushSemaphore?.Release();
                _flushSemaphore?.Dispose();
            }
        }

        /// <summary>
        /// Gets the current cache statistics if block caching is enabled.
        /// </summary>
        /// <returns>Cache statistics or null if caching is disabled.</returns>
        public CacheStats? GetCacheStats()
        {
            return _blockCache?.GetStats();
        }

        /// <summary>
        /// Clears the block cache, freeing cached memory.
        /// </summary>
        public void ClearCache()
        {
            _blockCache?.Clear();
        }

        /// <summary>
        /// Gets the current configuration of the database.
        /// </summary>
        /// <returns>The LSM configuration.</returns>
        public LSMConfiguration GetConfiguration()
        {
            return _config;
        }

        /// <summary>
        /// Gets statistics about the current state of the database.
        /// </summary>
        /// <returns>Database statistics including memtable size and SSTable counts.</returns>
        public DatabaseStats GetDatabaseStats()
        {
            lock (_memtableLock)
            {
                var activeMemtableSize = _activeMemtable?.Size ?? 0;
                var flushingMemtableSize = _flushingMemtable?.Size ?? 0;
                
                return new DatabaseStats
                {
                    ActiveMemtableSize = activeMemtableSize,
                    FlushingMemtableSize = flushingMemtableSize,
                    TotalMemtableSize = activeMemtableSize + flushingMemtableSize,
                    IsFlushingInProgress = _flushingMemtable != null
                };
            }
        }
    }

    /// <summary>
    /// Represents statistics about the current state of the database.
    /// </summary>
    public struct DatabaseStats
    {
        /// <summary>
        /// Size of the active memtable in bytes.
        /// </summary>
        public int ActiveMemtableSize { get; set; }

        /// <summary>
        /// Size of the flushing memtable in bytes (0 if no flush is in progress).
        /// </summary>
        public int FlushingMemtableSize { get; set; }

        /// <summary>
        /// Total memtable size (active + flushing) in bytes.
        /// </summary>
        public int TotalMemtableSize { get; set; }

        /// <summary>
        /// Indicates whether a flush operation is currently in progress.
        /// </summary>
        public bool IsFlushingInProgress { get; set; }
    }
}
