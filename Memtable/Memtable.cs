using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using LSMTree.Core;
using LSMTree.SkipList;
using LSMTree.WAL;

namespace LSMTree.Memtable
{
    public class Memtable : IMemtable
    {
        private readonly ISkipList _skipList;
        private readonly IWriteAheadLog _wal;
        private readonly object _lock = new object();
        private bool _readOnly;

        public bool IsReadOnly 
        { 
            get 
            { 
                lock (_lock) 
                { 
                    return _readOnly; 
                } 
            } 
        }

        public int Size => _skipList.Size;

        public Memtable(string walFilePath)
        {
            _skipList = new ConcurrentSkipList();
            _wal = new WriteAheadLog(walFilePath);
            _readOnly = false;
        }

        public Memtable(ISkipList skipList, IWriteAheadLog wal)
        {
            _skipList = skipList ?? throw new ArgumentNullException(nameof(skipList));
            _wal = wal ?? throw new ArgumentNullException(nameof(wal));
            _readOnly = false;
        }

        public async Task SetAsync(Entry entry)
        {
            lock (_lock)
            {
                if (_readOnly)
                    throw new InvalidOperationException("Cannot write to read-only memtable");
            }

            // Write to WAL first for durability
            await _wal.WriteAsync(entry);

            // Then write to skip list
            _skipList.Set(entry);
        }

        public (bool found, Entry entry) Get(string key)
        {
            return _skipList.Get(key);
        }

        public IEnumerable<Entry> GetAll()
        {
            return _skipList.GetAll();
        }

        public bool ShouldFlush(int threshold)
        {
            return Size >= threshold;
        }

        public void MakeReadOnly()
        {
            lock (_lock)
            {
                _readOnly = true;
            }
        }

        public async Task RecoverAsync()
        {
            var entries = await _wal.ReadAsync();
            
            foreach (var entry in entries)
            {
                _skipList.Set(entry);
            }
        }

        public async Task DeleteWalAsync()
        {
            await _wal.DeleteAsync();
        }

        public void Dispose()
        {
            _wal?.Dispose();
        }
    }

    public static class MemtableFactory
    {
        public static async Task<Memtable> CreateAsync(string walFilePath, bool recover = true)
        {
            var memtable = new Memtable(walFilePath);
            
            if (recover)
            {
                await memtable.RecoverAsync();
            }

            return memtable;
        }

        public static async Task<Memtable> CreateFromWalFilesAsync(string directory, string version)
        {
            var walFiles = Directory.GetFiles(directory, "*.wal")
                .Where(f => Path.GetFileNameWithoutExtension(f).StartsWith(version))
                .OrderBy(f => f)
                .ToList();

            if (!walFiles.Any())
            {
                var newWalPath = Path.Combine(directory, $"{version}_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}.wal");
                return await CreateAsync(newWalPath, false);
            }

            // Use the latest WAL file
            var latestWal = walFiles.Last();
            var memtable = await CreateAsync(latestWal, true);

            // Clean up older WAL files after recovery
            foreach (var oldWal in walFiles.Take(walFiles.Count - 1))
            {
                try
                {
                    File.Delete(oldWal);
                }
                catch
                {
                    // Ignore deletion errors
                }
            }

            return memtable;
        }
    }
}
