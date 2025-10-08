using System;
using System.Collections.Concurrent;
using System.Threading;
using LSMTree.SSTable;

namespace LSMTree.Core
{
    public interface ISSTableCache : IDisposable
    {
        SSTable.SSTable GetOrOpen(string filePath, IBlockCache? blockCache = null);
        void Remove(string filePath);
        void Clear();
    }

    public class SSTableCache : ISSTableCache
    {
        private readonly ConcurrentDictionary<string, CacheEntry> _cache;
        private readonly int _maxSize;
        private readonly Timer _cleanupTimer;
        private const int CleanupIntervalMs = 30000;
        private const int MaxIdleSeconds = 60;

        private class CacheEntry
        {
            public SSTable.SSTable SSTable { get; }
            public long LastAccessTicks { get; set; }

            public CacheEntry(SSTable.SSTable sstable)
            {
                SSTable = sstable;
                LastAccessTicks = DateTime.UtcNow.Ticks;
            }
        }

        public SSTableCache(int maxSize = 100)
        {
            _maxSize = maxSize;
            _cache = new ConcurrentDictionary<string, CacheEntry>();
            _cleanupTimer = new Timer(CleanupCallback, null, CleanupIntervalMs, CleanupIntervalMs);
        }

        public SSTable.SSTable GetOrOpen(string filePath, IBlockCache? blockCache = null)
        {
            if (_cache.TryGetValue(filePath, out var entry))
            {
                entry.LastAccessTicks = DateTime.UtcNow.Ticks;
                return entry.SSTable;
            }

            var sstable = SSTable.SSTable.OpenAsync(filePath, blockCache).GetAwaiter().GetResult();
            var newEntry = new CacheEntry(sstable);

            if (_cache.TryAdd(filePath, newEntry))
            {
                if (_cache.Count > _maxSize)
                {
                    EvictOldest();
                }
                return sstable;
            }

            if (_cache.TryGetValue(filePath, out entry))
            {
                sstable.Dispose();
                entry.LastAccessTicks = DateTime.UtcNow.Ticks;
                return entry.SSTable;
            }

            return sstable;
        }

        public void Remove(string filePath)
        {
            if (_cache.TryRemove(filePath, out var entry))
            {
                try
                {
                    entry.SSTable.Dispose();
                }
                catch
                {
                }
            }
        }

        public void Clear()
        {
            foreach (var entry in _cache.Values)
            {
                try
                {
                    entry.SSTable.Dispose();
                }
                catch
                {
                }
            }
            _cache.Clear();
        }

        private void CleanupCallback(object? state)
        {
            var now = DateTime.UtcNow.Ticks;
            var maxIdleTicks = TimeSpan.FromSeconds(MaxIdleSeconds).Ticks;

            foreach (var kvp in _cache)
            {
                if (now - kvp.Value.LastAccessTicks > maxIdleTicks)
                {
                    if (_cache.TryRemove(kvp.Key, out var entry))
                    {
                        try
                        {
                            entry.SSTable.Dispose();
                        }
                        catch
                        {
                        }
                    }
                }
            }
        }

        private void EvictOldest()
        {
            string? oldestKey = null;
            long oldestTicks = long.MaxValue;

            foreach (var kvp in _cache)
            {
                if (kvp.Value.LastAccessTicks < oldestTicks)
                {
                    oldestTicks = kvp.Value.LastAccessTicks;
                    oldestKey = kvp.Key;
                }
            }

            if (oldestKey != null)
            {
                Remove(oldestKey);
            }
        }

        public void Dispose()
        {
            _cleanupTimer?.Dispose();
            Clear();
        }
    }
}
