using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using LSMTree.Core;

namespace LSMTree.SSTable
{
    public class DataBlock
    {
        public List<Entry> Entries { get; private set; }
        public CompressionType CompressionType { get; set; }
        private static readonly ICompressor DefaultCompressor = CompressionFactory.Create(CompressionType.GZip);

        public DataBlock(CompressionType compressionType = CompressionType.GZip)
        {
            Entries = new List<Entry>();
            CompressionType = compressionType;
        }

        public DataBlock(IEnumerable<Entry> entries, CompressionType compressionType = CompressionType.GZip)
        {
            Entries = entries.ToList();
            CompressionType = compressionType;
        }

        public byte[] Encode()
        {
            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream);

            string previousKey = string.Empty;

            foreach (var entry in Entries)
            {
                int commonPrefixLength = GetCommonPrefixLength(previousKey, entry.Key);
                string suffix = entry.Key.Substring(commonPrefixLength);

                writer.Write((ushort)commonPrefixLength);
                
                var suffixBytes = Encoding.UTF8.GetBytes(suffix);
                writer.Write((ushort)suffixBytes.Length);
                writer.Write(suffixBytes);
                
                writer.Write(entry.Value.Length);
                writer.Write(entry.Value);
                
                writer.Write(entry.Tombstone);
                writer.Write(entry.Timestamp);

                previousKey = entry.Key;
            }

            var uncompressed = stream.ToArray();
            var compressor = CompressionFactory.Create(CompressionType);
            return compressor.Compress(uncompressed);
        }

        public void Decode(byte[] data)
        {
            var compressor = CompressionFactory.Create(CompressionType);
            var uncompressed = compressor.Decompress(data);
            DecodeEntries(uncompressed);
        }

        private void DecodeEntries(byte[] uncompressed)
        {
            using var uncompressedStream = new MemoryStream(uncompressed);
            using var uncompressedReader = new BinaryReader(uncompressedStream);

            Entries.Clear();
            string previousKey = string.Empty;

            while (uncompressedStream.Position < uncompressedStream.Length)
            {
                ushort commonPrefixLength = uncompressedReader.ReadUInt16();
                ushort suffixLength = uncompressedReader.ReadUInt16();
                
                var suffixBytes = uncompressedReader.ReadBytes(suffixLength);
                string suffix = Encoding.UTF8.GetString(suffixBytes);
                
                int valueLength = uncompressedReader.ReadInt32();
                var value = uncompressedReader.ReadBytes(valueLength);
                
                bool tombstone = uncompressedReader.ReadBoolean();
                long timestamp = uncompressedReader.ReadInt64();

                string key = previousKey.Substring(0, commonPrefixLength) + suffix;
                
                Entries.Add(new Entry(key, value, tombstone, timestamp));
                previousKey = key;
            }
        }

        public (bool found, Entry entry) Search(string key)
        {
            int left = 0, right = Entries.Count - 1;

            while (left <= right)
            {
                int mid = left + (right - left) / 2;
                int comparison = string.Compare(Entries[mid].Key, key, StringComparison.Ordinal);

                if (comparison == 0)
                    return (true, Entries[mid]);
                else if (comparison < 0)
                    left = mid + 1;
                else
                    right = mid - 1;
            }

            return (false, default);
        }

        private static int GetCommonPrefixLength(string str1, string str2)
        {
            int length = Math.Min(str1.Length, str2.Length);
            int i = 0;
            
            while (i < length && str1[i] == str2[i])
                i++;
                
            return i;
        }
    }

    public class IndexBlock
    {
        public BlockHandle DataBlockHandle { get; set; }
        public List<IndexEntry> Entries { get; private set; }

        public IndexBlock()
        {
            Entries = new List<IndexEntry>();
        }

        public (bool found, BlockHandle handle) Search(string key)
        {
            if (Entries.Count == 0)
                return (false, default);

            // Check if key is beyond the range of this SSTable
            if (string.Compare(key, Entries.Last().EndKey, StringComparison.Ordinal) > 0)
                return (false, default);

            // Binary search for the appropriate data block
            int left = 0, right = Entries.Count - 1;

            while (left <= right)
            {
                int mid = left + (right - left) / 2;
                var entry = Entries[mid];

                if (string.Compare(key, entry.StartKey, StringComparison.Ordinal) < 0)
                {
                    right = mid - 1;
                }
                else if (string.Compare(key, entry.EndKey, StringComparison.Ordinal) > 0)
                {
                    left = mid + 1;
                }
                else
                {
                    // Key is within this block's range
                    return (true, entry.DataHandle);
                }
            }

            // If not found in range, check if we should look in the next block
            if (left < Entries.Count)
            {
                return (true, Entries[left].DataHandle);
            }

            return (false, default);
        }

        public byte[] Encode()
        {
            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream);

            // Write data block handle
            writer.Write(DataBlockHandle.Offset);
            writer.Write(DataBlockHandle.Length);

            // Write number of entries
            writer.Write(Entries.Count);

            // Write each index entry
            foreach (var entry in Entries)
            {
                var startKeyBytes = Encoding.UTF8.GetBytes(entry.StartKey);
                var endKeyBytes = Encoding.UTF8.GetBytes(entry.EndKey);

                writer.Write(startKeyBytes.Length);
                writer.Write(startKeyBytes);
                writer.Write(endKeyBytes.Length);
                writer.Write(endKeyBytes);
                writer.Write(entry.DataHandle.Offset);
                writer.Write(entry.DataHandle.Length);
            }

            return stream.ToArray();
        }

        public void Decode(byte[] data)
        {
            using var stream = new MemoryStream(data);
            using var reader = new BinaryReader(stream);

            // Read data block handle
            ulong offset = reader.ReadUInt64();
            ulong length = reader.ReadUInt64();
            DataBlockHandle = new BlockHandle(offset, length);

            // Read number of entries
            int entryCount = reader.ReadInt32();
            Entries.Clear();

            // Read each index entry
            for (int i = 0; i < entryCount; i++)
            {
                int startKeyLength = reader.ReadInt32();
                var startKeyBytes = reader.ReadBytes(startKeyLength);
                string startKey = Encoding.UTF8.GetString(startKeyBytes);

                int endKeyLength = reader.ReadInt32();
                var endKeyBytes = reader.ReadBytes(endKeyLength);
                string endKey = Encoding.UTF8.GetString(endKeyBytes);

                ulong dataOffset = reader.ReadUInt64();
                ulong dataLength = reader.ReadUInt64();
                var dataHandle = new BlockHandle(dataOffset, dataLength);

                Entries.Add(new IndexEntry(startKey, endKey, dataHandle));
            }
        }
    }

    public class MetaBlock
    {
        public long CreatedUnix { get; set; }
        public ulong Level { get; set; }
        public int EntryCount { get; set; }
        public string? MinKey { get; set; }
        public string? MaxKey { get; set; }

        public MetaBlock()
        {
            CreatedUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        }

        public byte[] Encode()
        {
            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream);

            writer.Write(CreatedUnix);
            writer.Write(Level);
            writer.Write(EntryCount);
            
            writer.Write(MinKey ?? string.Empty);
            writer.Write(MaxKey ?? string.Empty);

            return stream.ToArray();
        }

        public void Decode(byte[] data)
        {
            using var stream = new MemoryStream(data);
            using var reader = new BinaryReader(stream);

            CreatedUnix = reader.ReadInt64();
            Level = reader.ReadUInt64();
            EntryCount = reader.ReadInt32();
            MinKey = reader.ReadString();
            MaxKey = reader.ReadString();
        }
    }

    public class Footer
    {
        public BlockHandle MetaBlockHandle { get; set; }
        public BlockHandle IndexBlockHandle { get; set; }
        public ulong Magic { get; set; }

        public const ulong MagicNumber = 0x1234567890ABCDEFU;
        public const int FooterSize = 8 + 8 + 8 + 8 + 8; // 5 * sizeof(ulong)

        public Footer()
        {
            Magic = MagicNumber;
        }

        public byte[] Encode()
        {
            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream);

            writer.Write(MetaBlockHandle.Offset);
            writer.Write(MetaBlockHandle.Length);
            writer.Write(IndexBlockHandle.Offset);
            writer.Write(IndexBlockHandle.Length);
            writer.Write(Magic);

            return stream.ToArray();
        }

        public void Decode(byte[] data)
        {
            if (data.Length != FooterSize)
                throw new ArgumentException($"Invalid footer size. Expected {FooterSize}, got {data.Length}");

            using var stream = new MemoryStream(data);
            using var reader = new BinaryReader(stream);

            ulong metaOffset = reader.ReadUInt64();
            ulong metaLength = reader.ReadUInt64();
            MetaBlockHandle = new BlockHandle(metaOffset, metaLength);

            ulong indexOffset = reader.ReadUInt64();
            ulong indexLength = reader.ReadUInt64();
            IndexBlockHandle = new BlockHandle(indexOffset, indexLength);

            Magic = reader.ReadUInt64();

            if (Magic != MagicNumber)
                throw new InvalidDataException($"Invalid magic number. Expected {MagicNumber:X}, got {Magic:X}");
        }
    }
}
