using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using LSMTree.Core;
using LSMTree.Memtable;
using LSMTree.SSTable;
using LSMTree.Compaction;

namespace LSMTree
{
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

        public static async Task<LSMTreeDB> OpenAsync(
            string directory, 
            LSMConfiguration? config = null)
        {
            var db = new LSMTreeDB(directory, config);
            await db.RecoverAsync();
            return db;
        }

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

        public Task CompactAsync()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(LSMTreeDB));

            return _levelManager.CompactAsync(0);
        }

        private async Task TriggerFlushAsync()
        {
            // Non-blocking flush trigger
            _ = Task.Run(async () =>
            {
                try
                {
                    await FlushAsync();
                }
                catch
                {
                    // Log error in production
                }
            });
        }

        private async Task FlushMemtableAsync()
        {
            IMemtable memtableToFlush;
            
            // Switch to new active memtable
            lock (_memtableLock)
            {
                if (_activeMemtable.Size == 0)
                    return; // Nothing to flush

                _activeMemtable.MakeReadOnly();
                _flushingMemtable = _activeMemtable;
                _activeMemtable = CreateNewMemtable();
            }

            memtableToFlush = _flushingMemtable!;

            try
            {
                // Get all entries from memtable
                var entries = memtableToFlush.GetAll();

                // Create SSTable file
                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                var sstableFile = Path.Combine(_directory, "levels", $"L0_{timestamp}.sst");
                
                await SSTable.SSTable.BuildAsync(sstableFile, entries, 0, _config.DataBlockSize, _config.CompressionType);

                // Add to level manager
                await _levelManager.AddSSTableAsync(sstableFile);

                // Clean up WAL after successful flush
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

        public CacheStats? GetCacheStats()
        {
            return _blockCache?.GetStats();
        }

        public void ClearCache()
        {
            _blockCache?.Clear();
        }

        public LSMConfiguration GetConfiguration()
        {
            return _config;
        }
    }
}
