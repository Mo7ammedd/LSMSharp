using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using LSMTree.Core;

namespace LSMTree.Core
{
    public interface ILSMTree : IDisposable
    {
        Task SetAsync(string key, byte[] value);

        Task<(bool found, byte[] value)> GetAsync(string key);

        Task DeleteAsync(string key);

        Task FlushAsync();

        Task CompactAsync();
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
    }

    public interface IBloomFilter
    {
        void Add(string key);

        bool Contains(string key);

        byte[] Serialize();

        void Deserialize(byte[] data);
    }
}
