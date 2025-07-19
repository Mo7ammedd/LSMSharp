using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using LSMTree.Core;
using LSMTree.SSTable;
using LSMTree.Utils;
using LSMTree.BloomFilter;

namespace LSMTree.Compaction
{
    public class LevelManager : IDisposable
    {
        private readonly string _directory;
        private readonly int _l0TargetNum;
        private readonly int _levelMultiplier;
        private readonly int _dataBlockSize;
        private readonly CompressionType _compressionType;
        private readonly IBlockCache? _blockCache;
        private readonly List<LinkedList<TableHandle>> _levels;
        private readonly object _lock = new object();
        private bool _disposed = false;

        public LevelManager(string directory, IBlockCache? blockCache = null, int l0TargetNum = 4, int levelMultiplier = 10, int dataBlockSize = 4096, CompressionType compressionType = CompressionType.GZip)
        {
            _directory = directory ?? throw new ArgumentNullException(nameof(directory));
            _blockCache = blockCache;
            _l0TargetNum = l0TargetNum;
            _levelMultiplier = levelMultiplier;
            _dataBlockSize = dataBlockSize;
            _compressionType = compressionType;
            _levels = new List<LinkedList<TableHandle>>();

            // Ensure directory exists
            if (!Directory.Exists(_directory))
            {
                Directory.CreateDirectory(_directory);
            }
        }

        public async Task AddSSTableAsync(string filePath)
        {
            using var sstable = await SSTable.SSTable.OpenAsync(filePath, _blockCache);
            
            // Build bloom filter from all entries
            var entries = (await sstable.GetAllEntriesAsync()).ToList();
            var bloomFilter = new BloomFilter.BloomFilter(Math.Max(1, entries.Count));
            foreach (var entry in entries)
            {
                bloomFilter.Add(entry.Key);
            }

            var handle = new TableHandle(
                GetNextLevelIndex(0),
                filePath,
                bloomFilter,
                new IndexBlock(), // This will be loaded when needed
                new MetaBlock { 
                    Level = 0,
                    EntryCount = entries.Count,
                    MinKey = sstable.MinKey,
                    MaxKey = sstable.MaxKey
                });

            lock (_lock)
            {
                EnsureLevelExists(0);
                _levels[0].AddLast(handle);
            }

            // Check if compaction is needed
            if (ShouldCompact(0))
            {
                await CompactAsync(0);
            }
        }

        public async Task<(bool found, Entry entry)> SearchAsync(string key)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(LevelManager));

            List<LinkedList<TableHandle>> levelsCopy;
            lock (_lock)
            {
                levelsCopy = _levels.Select(level => new LinkedList<TableHandle>(level)).ToList();
            }

            // Search from L0 to LN
            for (int level = 0; level < levelsCopy.Count; level++)
            {
                foreach (var handle in levelsCopy[level])
                {
                    // Check bloom filter first
                    if (!handle.BloomFilter.Contains(key))
                        continue;

                    // Check if key is in range
                    if (!string.IsNullOrEmpty(handle.MinKey) && !string.IsNullOrEmpty(handle.MaxKey))
                    {
                        if (string.Compare(key, handle.MinKey, StringComparison.Ordinal) < 0 ||
                            string.Compare(key, handle.MaxKey, StringComparison.Ordinal) > 0)
                            continue;
                    }

                    // Search in the SSTable
                    try
                    {
                        using var sstable = await SSTable.SSTable.OpenAsync(handle.FilePath, _blockCache);
                        var result = await sstable.SearchAsync(key);
                        if (result.found)
                        {
                            return result;
                        }
                    }
                    catch (FileNotFoundException)
                    {
                        // SSTable file was deleted during compaction, continue searching
                        continue;
                    }
                }
            }

            return (false, default);
        }

        public async Task CompactAsync(int level)
        {
            if (level == 0)
            {
                await CompactL0ToL1Async();
            }
            else
            {
                await CompactLevelAsync(level);
            }
        }

        private async Task CompactL0ToL1Async()
        {
            List<TableHandle> l0Tables;
            List<TableHandle> l1OverlapTables;

            lock (_lock)
            {
                if (_levels.Count == 0 || _levels[0].Count == 0)
                    return;

                // Get all L0 tables (they can have overlapping ranges)
                l0Tables = _levels[0].ToList();

                // Find L1 tables that overlap with L0 tables
                EnsureLevelExists(1);
                var minKey = l0Tables.Where(t => t.MinKey != null).Min(t => t.MinKey) ?? "";
                var maxKey = l0Tables.Where(t => t.MaxKey != null).Max(t => t.MaxKey) ?? "";
                l1OverlapTables = FindOverlappingTables(1, minKey, maxKey);
            }

            if (!l0Tables.Any())
                return;

            // Prepare entries for merging (L1 first, then L0 for proper precedence)
            var allEntries = new List<IEnumerable<Entry>>();

            // Add L1 entries first (older)
            foreach (var handle in l1OverlapTables)
            {
                try
                {
                    using var sstable = await SSTable.SSTable.OpenAsync(handle.FilePath, _blockCache);
                    var entries = await sstable.GetAllEntriesAsync();
                    allEntries.Add(entries);
                }
                catch (FileNotFoundException)
                {
                    // Table was already deleted, skip
                }
            }

            // Add L0 entries last (newer, higher precedence)
            foreach (var handle in l0Tables)
            {
                try
                {
                    using var sstable = await SSTable.SSTable.OpenAsync(handle.FilePath, _blockCache);
                    var entries = await sstable.GetAllEntriesAsync();
                    allEntries.Add(entries);
                }
                catch (FileNotFoundException)
                {
                    // Table was already deleted, skip
                }
            }

            // Merge entries
            var mergedEntries = KWayMerge.Merge(allEntries.ToArray());

            if (!mergedEntries.Any())
                return;

            // Create new SSTable in L1
            var newFilePath = GenerateSSTablePath(1);
            var newSSTable = await SSTable.SSTable.BuildAsync(newFilePath, mergedEntries, 1, _dataBlockSize, _compressionType);

            // Update level structure
            lock (_lock)
            {
                // Remove old tables from levels
                foreach (var handle in l0Tables)
                {
                    _levels[0].Remove(handle);
                }

                foreach (var handle in l1OverlapTables)
                {
                    _levels[1].Remove(handle);
                }

                // Add new table to L1
                var bloomFilter = new BloomFilter.BloomFilter(Math.Max(1, mergedEntries.Count()));
                string? minKey = null;
                string? maxKey = null;
                
                foreach (var entry in mergedEntries)
                {
                    bloomFilter.Add(entry.Key);
                    if (minKey == null || string.Compare(entry.Key, minKey, StringComparison.Ordinal) < 0)
                        minKey = entry.Key;
                    if (maxKey == null || string.Compare(entry.Key, maxKey, StringComparison.Ordinal) > 0)
                        maxKey = entry.Key;
                }
                
                var newHandle = new TableHandle(
                    GetNextLevelIndex(1),
                    newFilePath,
                    bloomFilter,
                    new IndexBlock(),
                    new MetaBlock { 
                        Level = 1,
                        EntryCount = mergedEntries.Count(),
                        MinKey = minKey,
                        MaxKey = maxKey
                    });

                _levels[1].AddLast(newHandle);
            }

            // Delete old SSTable files
            var filesToDelete = l0Tables.Concat(l1OverlapTables).Select(h => h.FilePath).ToList();
            await DeleteSSTableFilesAsync(filesToDelete);

            newSSTable.Dispose();

            // Check if further compaction is needed
            if (ShouldCompact(1))
            {
                await CompactAsync(1);
            }
        }

        private async Task CompactLevelAsync(int level)
        {
            TableHandle? selectedTable;
            List<TableHandle> nextLevelOverlapTables;

            lock (_lock)
            {
                if (_levels.Count <= level || _levels[level].Count == 0)
                    return;

                // Select the oldest table from current level
                selectedTable = _levels[level].First?.Value;
                if (selectedTable == null)
                    return;

                // Find overlapping tables in next level
                EnsureLevelExists(level + 1);
                nextLevelOverlapTables = FindOverlappingTables(level + 1, selectedTable.MinKey, selectedTable.MaxKey);
            }

            // Prepare entries for merging
            var allEntries = new List<IEnumerable<Entry>>();

            // Add next level entries first (older)
            foreach (var handle in nextLevelOverlapTables)
            {
                try
                {
                    using var sstable = await SSTable.SSTable.OpenAsync(handle.FilePath, _blockCache);
                    var entries = await sstable.GetAllEntriesAsync();
                    allEntries.Add(entries);
                }
                catch (FileNotFoundException)
                {
                    // Table was already deleted, skip
                }
            }

            // Add current level entry last (newer)
            try
            {
                using var sstable = await SSTable.SSTable.OpenAsync(selectedTable.FilePath, _blockCache);
                var entries = await sstable.GetAllEntriesAsync();
                allEntries.Add(entries);
            }
            catch (FileNotFoundException)
            {
                return; // Table was already deleted
            }

            // Merge entries
            var mergedEntries = KWayMerge.Merge(allEntries.ToArray());

            if (!mergedEntries.Any())
                return;

            // Create new SSTable(s) in next level
            var newFilePath = GenerateSSTablePath(level + 1);
            var newSSTable = await SSTable.SSTable.BuildAsync(newFilePath, mergedEntries, level + 1, _dataBlockSize, _compressionType);

            // Update level structure
            lock (_lock)
            {
                // Remove old tables
                _levels[level].Remove(selectedTable);
                foreach (var handle in nextLevelOverlapTables)
                {
                    _levels[level + 1].Remove(handle);
                }

                // Add new table
                var bloomFilter = new BloomFilter.BloomFilter(Math.Max(1, mergedEntries.Count()));
                string? minKey = null;
                string? maxKey = null;
                
                foreach (var entry in mergedEntries)
                {
                    bloomFilter.Add(entry.Key);
                    if (minKey == null || string.Compare(entry.Key, minKey, StringComparison.Ordinal) < 0)
                        minKey = entry.Key;
                    if (maxKey == null || string.Compare(entry.Key, maxKey, StringComparison.Ordinal) > 0)
                        maxKey = entry.Key;
                }
                
                var newHandle = new TableHandle(
                    GetNextLevelIndex(level + 1),
                    newFilePath,
                    bloomFilter,
                    new IndexBlock(),
                    new MetaBlock { 
                        Level = (ulong)(level + 1),
                        EntryCount = mergedEntries.Count(),
                        MinKey = minKey,
                        MaxKey = maxKey
                    });

                _levels[level + 1].AddLast(newHandle);
            }

            // Delete old SSTable files
            var filesToDelete = nextLevelOverlapTables.Select(h => h.FilePath).Append(selectedTable.FilePath).ToList();
            await DeleteSSTableFilesAsync(filesToDelete);

            newSSTable.Dispose();

            // Check if further compaction is needed
            if (ShouldCompact(level + 1))
            {
                await CompactAsync(level + 1);
            }
        }

        private bool ShouldCompact(int level)
        {
            lock (_lock)
            {
                if (_levels.Count <= level)
                    return false;

                if (level == 0)
                {
                    return _levels[0].Count >= _l0TargetNum;
                }
                else
                {
                    int targetSize = (int)Math.Pow(_levelMultiplier, level);
                    return _levels[level].Count >= targetSize;
                }
            }
        }

        private List<TableHandle> FindOverlappingTables(int level, string startKey, string endKey)
        {
            if (_levels.Count <= level)
                return new List<TableHandle>();

            return _levels[level]
                .Where(handle => OverlapsWith(handle.MinKey, handle.MaxKey, startKey, endKey))
                .ToList();
        }

        private static bool OverlapsWith(string minKey1, string maxKey1, string minKey2, string maxKey2)
        {
            if (string.IsNullOrEmpty(minKey1) || string.IsNullOrEmpty(maxKey1) ||
                string.IsNullOrEmpty(minKey2) || string.IsNullOrEmpty(maxKey2))
                return true; // Conservative approach

            return !(string.Compare(maxKey2, minKey1, StringComparison.Ordinal) < 0 ||
                     string.Compare(minKey2, maxKey1, StringComparison.Ordinal) > 0);
        }

        private void EnsureLevelExists(int level)
        {
            while (_levels.Count <= level)
            {
                _levels.Add(new LinkedList<TableHandle>());
            }
        }

        private int GetNextLevelIndex(int level)
        {
            if (_levels.Count <= level || _levels[level].Count == 0)
                return 0;

            return _levels[level].Max(h => h.LevelIndex) + 1;
        }

        private string GenerateSSTablePath(int level)
        {
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var index = GetNextLevelIndex(level);
            return Path.Combine(_directory, $"L{level}_{index}_{timestamp}.sst");
        }

        private async Task DeleteSSTableFilesAsync(IEnumerable<string> filePaths)
        {
            await Task.Run(() =>
            {
                foreach (var filePath in filePaths)
                {
                    try
                    {
                        if (File.Exists(filePath))
                        {
                            File.Delete(filePath);
                        }
                    }
                    catch
                    {
                        // Ignore deletion errors
                    }
                }
            });
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                // Cleanup any resources if needed
                _disposed = true;
            }
        }
    }
}
