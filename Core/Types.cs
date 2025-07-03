using System;

namespace LSMTree.Core
{
    public struct Entry : IComparable<Entry>, IEquatable<Entry>
    {
        public string Key { get; }
        public byte[] Value { get; }
        public bool Tombstone { get; }
        public long Timestamp { get; }

        public Entry(string key, byte[] value, bool tombstone = false, long timestamp = 0)
        {
            Key = key ?? throw new ArgumentNullException(nameof(key));
            Value = value ?? Array.Empty<byte>();
            Tombstone = tombstone;
            Timestamp = timestamp == 0 ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() : timestamp;
        }

        public int CompareTo(Entry other)
        {
            int keyComparison = string.Compare(Key, other.Key, StringComparison.Ordinal);
            if (keyComparison != 0)
                return keyComparison;
            
            // If keys are equal, newer entries (higher timestamp) come first
            return other.Timestamp.CompareTo(Timestamp);
        }

        public bool Equals(Entry other)
        {
            return Key == other.Key && Timestamp == other.Timestamp;
        }

        public override bool Equals(object? obj)
        {
            return obj is Entry entry && Equals(entry);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Key, Timestamp);
        }

        public static bool operator ==(Entry left, Entry right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(Entry left, Entry right)
        {
            return !left.Equals(right);
        }
    }

    public struct BlockHandle
    {
        public ulong Offset { get; }
        public ulong Length { get; }

        public BlockHandle(ulong offset, ulong length)
        {
            Offset = offset;
            Length = length;
        }
    }

    public struct IndexEntry
    {
        public string StartKey { get; }
        public string EndKey { get; }
        public BlockHandle DataHandle { get; }

        public IndexEntry(string startKey, string endKey, BlockHandle dataHandle)
        {
            StartKey = startKey ?? throw new ArgumentNullException(nameof(startKey));
            EndKey = endKey ?? throw new ArgumentNullException(nameof(endKey));
            DataHandle = dataHandle;
        }
    }
}
