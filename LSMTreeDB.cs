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
        private readonly int _memtableThreshold;
        private readonly int _dataBlockSize;
        private readonly LevelManager _levelManager;
        private readonly SemaphoreSlim _flushSemaphore;
        private readonly object _memtableLock = new object();

        private IMemtable _activeMemtable;
        private IMemtable? _flushingMemtable;
        private long _nextWalId;
        private bool _disposed = false;

        public LSMTreeDB(string directory, int memtableThreshold = 1024 * 1024, int dataBlockSize = 4096)
        {
            _directory = directory ?? throw new ArgumentNullException(nameof(directory));
            _memtableThreshold = memtableThreshold;
            _dataBlockSize = dataBlockSize;

            // Ensure directory exists
            if (!Directory.Exists(_directory))
            {
                Directory.CreateDirectory(_directory);
            }

            _levelManager = new LevelManager(Path.Combine(_directory, "levels"));
            _flushSemaphore = new SemaphoreSlim(1, 1);
            _nextWalId = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // Initialize active memtable
            _activeMemtable = CreateNewMemtable();
        }

        public static async Task<LSMTreeDB> OpenAsync(
            string directory, 
            int memtableThreshold = 1024 * 1024, 
            int dataBlockSize = 4096)
        {
            var db = new LSMTreeDB(directory, memtableThreshold, dataBlockSize);
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

            await memtableToUse.SetAsync(entry);

            // Check if flush is needed
            if (memtableToUse.ShouldFlush(_memtableThreshold))
            {
                await TriggerFlushAsync();
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

            // Check if flush is needed
            if (memtableToUse.ShouldFlush(_memtableThreshold))
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
                
                await SSTable.SSTable.BuildAsync(sstableFile, entries, 0, _dataBlockSize);

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
                        
                        await SSTable.SSTable.BuildAsync(sstableFile, entries, 0, _dataBlockSize);
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

            _disposed = true;

            try
            {
                // Flush any remaining data
                await FlushAsync();
            }
            catch
            {
                // Ignore flush errors during disposal
            }

            _activeMemtable?.Dispose();
            _flushingMemtable?.Dispose();
            _levelManager?.Dispose();
            _flushSemaphore?.Dispose();
        }
    }
}
