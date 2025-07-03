using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using LSMTree.Core;
using LSMTree.BloomFilter;

namespace LSMTree.SSTable
{
    public class SSTable : IDisposable
    {
        private readonly string _filePath;
        private readonly FileStream? _fileStream;
        internal IndexBlock? _indexBlock;
        internal MetaBlock? _metaBlock;
        internal Footer? _footer;
        private bool _disposed = false;

        public string FilePath => _filePath;
        public int Level => (int)(_metaBlock?.Level ?? 0);
        public string? MinKey => _metaBlock?.MinKey;
        public string? MaxKey => _metaBlock?.MaxKey;
        public int EntryCount => _metaBlock?.EntryCount ?? 0;

        private SSTable(string filePath, FileStream? fileStream = null)
        {
            _filePath = filePath;
            _fileStream = fileStream;
        }

        public async Task<(bool found, Entry entry)> SearchAsync(string key)
        {
            if (_disposed || _indexBlock == null)
                return (false, default);

            // Check if key is in range
            if (!string.IsNullOrEmpty(MinKey) && !string.IsNullOrEmpty(MaxKey))
            {
                if (string.Compare(key, MinKey, StringComparison.Ordinal) < 0 ||
                    string.Compare(key, MaxKey, StringComparison.Ordinal) > 0)
                    return (false, default);
            }

            // Find the data block that might contain the key
            var (found, blockHandle) = _indexBlock.Search(key);
            if (!found)
                return (false, default);

            // Read and search the data block
            var dataBlock = await ReadDataBlockAsync(blockHandle);
            return dataBlock.Search(key);
        }

        public async Task<IEnumerable<Entry>> GetAllEntriesAsync()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(SSTable));

            if (_indexBlock == null || _indexBlock.Entries == null)
                return new List<Entry>();

            var allEntries = new List<Entry>();

            foreach (var indexEntry in _indexBlock.Entries)
            {
                var dataBlock = await ReadDataBlockAsync(indexEntry.DataHandle);
                allEntries.AddRange(dataBlock.Entries);
            }

            return allEntries;
        }

        private async Task<DataBlock> ReadDataBlockAsync(BlockHandle handle)
        {
            if (_fileStream == null)
                throw new InvalidOperationException("File stream is not available");

            var buffer = new byte[handle.Length];
            _fileStream.Seek((long)handle.Offset, SeekOrigin.Begin);
            
            await _fileStream.ReadAsync(buffer, 0, (int)handle.Length);
            
            var dataBlock = new DataBlock();
            dataBlock.Decode(buffer);
            return dataBlock;
        }

        public static async Task<SSTable> OpenAsync(string filePath)
        {
            if (!File.Exists(filePath))
                throw new FileNotFoundException($"SSTable file not found: {filePath}");

            var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            
            try
            {
                // Read footer
                if (stream.Length < Footer.FooterSize)
                    throw new InvalidDataException("File too small to contain valid SSTable");

                stream.Seek(-Footer.FooterSize, SeekOrigin.End);
                var footerBytes = new byte[Footer.FooterSize];
                await stream.ReadAsync(footerBytes, 0, Footer.FooterSize);

                var footer = new Footer();
                footer.Decode(footerBytes);

                // Read meta block
                var metaBytes = new byte[footer.MetaBlockHandle.Length];
                stream.Seek((long)footer.MetaBlockHandle.Offset, SeekOrigin.Begin);
                await stream.ReadAsync(metaBytes, 0, (int)footer.MetaBlockHandle.Length);

                var metaBlock = new MetaBlock();
                metaBlock.Decode(metaBytes);

                // Read index block
                var indexBytes = new byte[footer.IndexBlockHandle.Length];
                stream.Seek((long)footer.IndexBlockHandle.Offset, SeekOrigin.Begin);
                await stream.ReadAsync(indexBytes, 0, (int)footer.IndexBlockHandle.Length);

                var indexBlock = new IndexBlock();
                indexBlock.Decode(indexBytes);

                // Create SSTable instance with loaded metadata
                var sstable = new SSTable(filePath, stream);
                sstable._indexBlock = indexBlock;
                sstable._metaBlock = metaBlock;
                sstable._footer = footer;

                return sstable;
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }

        public static async Task<SSTable> BuildAsync(
            string filePath, 
            IEnumerable<Entry> entries, 
            int level = 0, 
            int dataBlockSize = 4096)
        {
            var sortedEntries = entries.OrderBy(e => e.Key, StringComparer.Ordinal).ToList();
            
            if (!sortedEntries.Any())
                throw new ArgumentException("Cannot build SSTable with no entries");

            // Ensure directory exists
            var directory = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            using var stream = new FileStream(filePath, FileMode.Create, FileAccess.Write);
            
            var indexBlock = new IndexBlock();
            var metaBlock = new MetaBlock
            {
                Level = (ulong)level,
                EntryCount = sortedEntries.Count,
                MinKey = sortedEntries.First().Key,
                MaxKey = sortedEntries.Last().Key
            };

            // Write data blocks
            var currentBlockEntries = new List<Entry>();
            int currentBlockSize = 0;

            foreach (var entry in sortedEntries)
            {
                var entrySize = EstimateEntrySize(entry);
                
                if (currentBlockSize + entrySize > dataBlockSize && currentBlockEntries.Any())
                {
                    // Write current block
                    await WriteDataBlock(stream, currentBlockEntries, indexBlock);
                    currentBlockEntries.Clear();
                    currentBlockSize = 0;
                }

                currentBlockEntries.Add(entry);
                currentBlockSize += entrySize;
            }

            // Write final block
            if (currentBlockEntries.Any())
            {
                await WriteDataBlock(stream, currentBlockEntries, indexBlock);
            }

            // Write meta block
            var metaBytes = metaBlock.Encode();
            var metaOffset = stream.Position;
            await stream.WriteAsync(metaBytes, 0, metaBytes.Length);
            var metaHandle = new BlockHandle((ulong)metaOffset, (ulong)metaBytes.Length);

            // Write index block
            var indexBytes = indexBlock.Encode();
            var indexOffset = stream.Position;
            await stream.WriteAsync(indexBytes, 0, indexBytes.Length);
            var indexHandle = new BlockHandle((ulong)indexOffset, (ulong)indexBytes.Length);

            // Write footer
            var footer = new Footer
            {
                MetaBlockHandle = metaHandle,
                IndexBlockHandle = indexHandle
            };
            var footerBytes = footer.Encode();
            await stream.WriteAsync(footerBytes, 0, footerBytes.Length);

            await stream.FlushAsync();

            return new SSTable(filePath);
        }

        private static async Task WriteDataBlock(
            FileStream stream, 
            List<Entry> entries, 
            IndexBlock indexBlock)
        {
            var dataBlock = new DataBlock(entries);
            var dataBytes = dataBlock.Encode();
            
            var offset = stream.Position;
            await stream.WriteAsync(dataBytes, 0, dataBytes.Length);
            
            var handle = new BlockHandle((ulong)offset, (ulong)dataBytes.Length);
            var indexEntry = new IndexEntry(
                entries.First().Key, 
                entries.Last().Key, 
                handle);
            
            indexBlock.Entries.Add(indexEntry);
        }

        private static int EstimateEntrySize(Entry entry)
        {
            return sizeof(long) + // timestamp
                   sizeof(bool) + // tombstone
                   (entry.Key?.Length ?? 0) * sizeof(char) + // key
                   (entry.Value?.Length ?? 0) + // value
                   64; // overhead estimation
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _fileStream?.Dispose();
                _disposed = true;
            }
        }
    }

    public class TableHandle
    {
        public int LevelIndex { get; set; }
        public string FilePath { get; set; }
        public IBloomFilter BloomFilter { get; set; }
        public IndexBlock DataBlockIndex { get; set; }
        public MetaBlock Metadata { get; set; }

        public TableHandle(
            int levelIndex, 
            string filePath, 
            IBloomFilter bloomFilter, 
            IndexBlock dataBlockIndex,
            MetaBlock metadata)
        {
            LevelIndex = levelIndex;
            FilePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
            BloomFilter = bloomFilter ?? throw new ArgumentNullException(nameof(bloomFilter));
            DataBlockIndex = dataBlockIndex ?? throw new ArgumentNullException(nameof(dataBlockIndex));
            Metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
        }

        public string? MinKey => Metadata.MinKey;
        public string? MaxKey => Metadata.MaxKey;
        public int Level => (int)Metadata.Level;
    }
}
