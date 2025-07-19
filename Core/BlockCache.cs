using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using LSMTree.Core;
using LSMTree.SSTable;

namespace LSMTree.Core
{
    public interface IBlockCache
    {
        bool TryGet(string key, out DataBlock block);
        void Put(string key, DataBlock block);
        void Remove(string key);
        void Clear();
        long Size { get; }
        long MaxSize { get; }
        CacheStats GetStats();
    }

    public struct CacheStats
    {
        public long Hits { get; set; }
        public long Misses { get; set; }
        public long Evictions { get; set; }
        public long Size { get; set; }
        public long MaxSize { get; set; }
        public double HitRatio => Hits + Misses > 0 ? (double)Hits / (Hits + Misses) : 0.0;
    }

    public class LRUBlockCache : IBlockCache
    {
        private readonly ConcurrentDictionary<string, CacheNode> _cache;
        private readonly ReaderWriterLockSlim _lruLock;
        private readonly long _maxSize;
        private CacheNode? _head;
        private CacheNode? _tail;
        private long _currentSize;
        private long _hits;
        private long _misses;
        private long _evictions;

        public long Size => _currentSize;
        public long MaxSize => _maxSize;

        private class CacheNode
        {
            public string Key { get; }
            public DataBlock Block { get; }
            public long Size { get; }
            public CacheNode? Prev { get; set; }
            public CacheNode? Next { get; set; }

            public CacheNode(string key, DataBlock block, long size)
            {
                Key = key;
                Block = block;
                Size = size;
            }
        }

        public LRUBlockCache(long maxSizeBytes = 64 * 1024 * 1024)
        {
            _cache = new ConcurrentDictionary<string, CacheNode>();
            _lruLock = new ReaderWriterLockSlim();
            _maxSize = maxSizeBytes;
            _currentSize = 0;
            _hits = 0;
            _misses = 0;
            _evictions = 0;
        }

        public bool TryGet(string key, out DataBlock block)
        {
            if (_cache.TryGetValue(key, out var node))
            {
                Interlocked.Increment(ref _hits);
                MoveToHead(node);
                block = node.Block;
                return true;
            }

            Interlocked.Increment(ref _misses);
            block = null!;
            return false;
        }

        public void Put(string key, DataBlock block)
        {
            var blockSize = EstimateBlockSize(block);
            
            if (blockSize > _maxSize)
                return;

            if (_cache.TryGetValue(key, out var existingNode))
            {
                MoveToHead(existingNode);
                return;
            }

            var newNode = new CacheNode(key, block, blockSize);
            
            _lruLock.EnterWriteLock();
            try
            {
                if (_cache.TryAdd(key, newNode))
                {
                    AddToHead(newNode);
                    Interlocked.Add(ref _currentSize, blockSize);
                    
                    while (_currentSize > _maxSize && _tail != null)
                    {
                        EvictLRU();
                    }
                }
            }
            finally
            {
                _lruLock.ExitWriteLock();
            }
        }

        public void Remove(string key)
        {
            if (_cache.TryRemove(key, out var node))
            {
                _lruLock.EnterWriteLock();
                try
                {
                    RemoveFromList(node);
                    Interlocked.Add(ref _currentSize, -node.Size);
                }
                finally
                {
                    _lruLock.ExitWriteLock();
                }
            }
        }

        public void Clear()
        {
            _lruLock.EnterWriteLock();
            try
            {
                _cache.Clear();
                _head = null;
                _tail = null;
                _currentSize = 0;
                _evictions = 0;
            }
            finally
            {
                _lruLock.ExitWriteLock();
            }
        }

        public CacheStats GetStats()
        {
            return new CacheStats
            {
                Hits = _hits,
                Misses = _misses,
                Evictions = _evictions,
                Size = _currentSize,
                MaxSize = _maxSize
            };
        }

        private void MoveToHead(CacheNode node)
        {
            _lruLock.EnterWriteLock();
            try
            {
                RemoveFromList(node);
                AddToHead(node);
            }
            finally
            {
                _lruLock.ExitWriteLock();
            }
        }

        private void AddToHead(CacheNode node)
        {
            node.Prev = null;
            node.Next = _head;

            if (_head != null)
                _head.Prev = node;

            _head = node;

            if (_tail == null)
                _tail = node;
        }

        private void RemoveFromList(CacheNode node)
        {
            if (node.Prev != null)
                node.Prev.Next = node.Next;
            else
                _head = node.Next;

            if (node.Next != null)
                node.Next.Prev = node.Prev;
            else
                _tail = node.Prev;
        }

        private void EvictLRU()
        {
            if (_tail == null) return;

            var nodeToEvict = _tail;
            
            if (_cache.TryRemove(nodeToEvict.Key, out _))
            {
                RemoveFromList(nodeToEvict);
                Interlocked.Add(ref _currentSize, -nodeToEvict.Size);
                Interlocked.Increment(ref _evictions);
            }
        }

        private static long EstimateBlockSize(DataBlock block)
        {
            if (block?.Entries == null) return 0;
            
            long size = 0;
            foreach (var entry in block.Entries)
            {
                size += (entry.Key?.Length ?? 0) * sizeof(char);
                size += entry.Value?.Length ?? 0;
                size += sizeof(long) + sizeof(bool);
            }
            return size + 128;
        }
    }

    public class BlockCacheKey
    {
        public string SSTablePath { get; }
        public ulong BlockOffset { get; }

        public BlockCacheKey(string sstablePath, ulong blockOffset)
        {
            SSTablePath = sstablePath ?? throw new ArgumentNullException(nameof(sstablePath));
            BlockOffset = blockOffset;
        }

        public override string ToString()
        {
            return $"{SSTablePath}:{BlockOffset}";
        }

        public override bool Equals(object? obj)
        {
            return obj is BlockCacheKey other &&
                   SSTablePath == other.SSTablePath &&
                   BlockOffset == other.BlockOffset;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(SSTablePath, BlockOffset);
        }
    }
}
