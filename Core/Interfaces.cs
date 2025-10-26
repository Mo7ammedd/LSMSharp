using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using LSMTree.Core;

namespace LSMTree.Core
{
    /// <summary>
    /// Represents the main interface for an LSM-Tree storage engine.
    /// </summary>
    public interface ILSMTree : IDisposable
    {
        /// <summary>
        /// Asynchronously sets a key-value pair in the database.
        /// </summary>
        /// <param name="key">The key to set.</param>
        /// <param name="value">The value to associate with the key.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task SetAsync(string key, byte[] value);

        /// <summary>
        /// Asynchronously retrieves the value associated with the specified key.
        /// </summary>
        /// <param name="key">The key to retrieve.</param>
        /// <returns>A task containing a tuple with a boolean indicating if the key was found and the associated value.</returns>
        Task<(bool found, byte[] value)> GetAsync(string key);

        /// <summary>
        /// Asynchronously deletes a key from the database by writing a tombstone.
        /// </summary>
        /// <param name="key">The key to delete.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task DeleteAsync(string key);

        /// <summary>
        /// Asynchronously flushes the active memtable to disk as an SSTable.
        /// </summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task FlushAsync();

        /// <summary>
        /// Asynchronously triggers compaction of SSTables to merge and eliminate obsolete data.
        /// </summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task CompactAsync();

        /// <summary>
        /// Asynchronously performs a range scan between the specified start and end keys (inclusive).
        /// </summary>
        /// <param name="startKey">The starting key of the range (inclusive).</param>
        /// <param name="endKey">The ending key of the range (inclusive).</param>
        /// <returns>An async enumerable of key-value pairs within the range.</returns>
        IAsyncEnumerable<(string key, byte[] value)> RangeAsync(string startKey, string endKey);
    }

    public interface ISkipList
    {
        void Set(Entry entry);

        (bool found, Entry entry) Get(string key);

        IEnumerable<Entry> GetAll();

        int Size { get; }

        bool IsEmpty { get; }
    }

    public interface IWriteAheadLog : IDisposable
    {
        Task WriteAsync(params Entry[] entries);

        Task<IEnumerable<Entry>> ReadAsync();

        Task DeleteAsync();

        Task SyncAsync();
    }

    public interface IMemtable : IDisposable
    {
        Task SetAsync(Entry entry);

        (bool found, Entry entry) Get(string key);

        IEnumerable<Entry> GetAll();

        bool ShouldFlush(int threshold);

        void MakeReadOnly();

        bool IsReadOnly { get; }

        int Size { get; }

        Task RecoverAsync();

        Task SyncWalAsync();
    }

    public interface IBloomFilter
    {
        void Add(string key);

        bool Contains(string key);

        byte[] Serialize();

        void Deserialize(byte[] data);
    }
}
