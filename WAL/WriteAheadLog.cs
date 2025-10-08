using System;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Collections.Concurrent;
using LSMTree.Core;

namespace LSMTree.WAL
{
    public class WriteAheadLog : IWriteAheadLog
    {
        private readonly string _filePath;
        private readonly FileStream _fileStream;
        private readonly BinaryWriter _writer;
        private readonly ConcurrentQueue<Entry> _writeQueue;
        private readonly SemaphoreSlim _flushSemaphore;
        private readonly Timer _flushTimer;
        private readonly object _writeLock = new object();
        private bool _disposed = false;
        private const int FlushIntervalMs = 100;
        private const int MaxBatchSize = 100;

        public WriteAheadLog(string filePath)
        {
            _filePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
            
            var directory = Path.GetDirectoryName(_filePath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            _fileStream = new FileStream(_filePath, FileMode.Append, FileAccess.Write, FileShare.Read, 64 * 1024);
            _writer = new BinaryWriter(_fileStream, Encoding.UTF8, leaveOpen: true);
            _writeQueue = new ConcurrentQueue<Entry>();
            _flushSemaphore = new SemaphoreSlim(1, 1);
            _flushTimer = new Timer(FlushCallback, null, FlushIntervalMs, FlushIntervalMs);
        }

        public Task WriteAsync(params Entry[] entries)
        {
            if (entries == null || entries.Length == 0)
                return Task.CompletedTask;

            if (_disposed)
                throw new ObjectDisposedException(nameof(WriteAheadLog));

            foreach (var entry in entries)
            {
                _writeQueue.Enqueue(entry);
            }

            if (_writeQueue.Count >= MaxBatchSize)
            {
                return FlushBatchAsync();
            }

            return Task.CompletedTask;
        }

        private void FlushCallback(object? state)
        {
            if (!_disposed && !_writeQueue.IsEmpty)
            {
                _ = FlushBatchAsync();
            }
        }

        private async Task FlushBatchAsync()
        {
            if (!await _flushSemaphore.WaitAsync(0))
                return;

            try
            {
                if (_disposed)
                    return;

                var batch = new List<Entry>();
                while (batch.Count < MaxBatchSize && _writeQueue.TryDequeue(out var entry))
                {
                    batch.Add(entry);
                }

                if (batch.Count == 0)
                    return;

                lock (_writeLock)
                {
                    if (_disposed)
                        return;

                    foreach (var entry in batch)
                    {
                        WriteEntry(entry);
                    }
                    _writer.Flush();
                    _fileStream.Flush(true);
                }
            }
            finally
            {
                _flushSemaphore.Release();
            }
        }

        public Task<IEnumerable<Entry>> ReadAsync()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(WriteAheadLog));

            var entries = new List<Entry>();

            if (!File.Exists(_filePath))
                return Task.FromResult<IEnumerable<Entry>>(entries);

            using var readStream = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var reader = new BinaryReader(readStream, Encoding.UTF8);

            while (readStream.Position < readStream.Length)
            {
                try
                {
                    var entry = ReadEntry(reader);
                    entries.Add(entry);
                }
                catch (EndOfStreamException)
                {
                    break;
                }
                catch (Exception)
                {
                    break;
                }
            }

            return Task.FromResult<IEnumerable<Entry>>(entries);
        }

        public async Task DeleteAsync()
        {
            if (_disposed)
                return;

            await _flushSemaphore.WaitAsync();
            try
            {
                lock (_writeLock)
                {
                    if (!_disposed)
                    {
                        _flushTimer?.Dispose();
                        _writer?.Close();
                        _fileStream?.Close();
                        _disposed = true;
                        
                        if (File.Exists(_filePath))
                        {
                            File.Delete(_filePath);
                        }
                    }
                }
            }
            finally
            {
                _flushSemaphore.Release();
            }
        }

        public async Task SyncAsync()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(WriteAheadLog));

            await FlushBatchAsync();
        }

        private void WriteEntry(Entry entry)
        {
            var keyBytes = Encoding.UTF8.GetBytes(entry.Key);
            
            _writer.Write(keyBytes.Length);
            _writer.Write(keyBytes);
            _writer.Write(entry.Value.Length);
            _writer.Write(entry.Value);
            _writer.Write(entry.Tombstone);
            _writer.Write(entry.Timestamp);
        }

        private Entry ReadEntry(BinaryReader reader)
        {
            int keyLength = reader.ReadInt32();
            var keyBytes = reader.ReadBytes(keyLength);
            string key = Encoding.UTF8.GetString(keyBytes);

            int valueLength = reader.ReadInt32();
            var value = reader.ReadBytes(valueLength);

            bool tombstone = reader.ReadBoolean();
            long timestamp = reader.ReadInt64();

            return new Entry(key, value, tombstone, timestamp);
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _flushSemaphore.Wait();
            try
            {
                lock (_writeLock)
                {
                    if (!_disposed)
                    {
                        _flushTimer?.Dispose();
                        
                        while (_writeQueue.TryDequeue(out var entry))
                        {
                            WriteEntry(entry);
                        }
                        
                        _writer?.Flush();
                        _fileStream?.Flush(true);
                        _writer?.Dispose();
                        _fileStream?.Dispose();
                        _disposed = true;
                    }
                }
            }
            finally
            {
                _flushSemaphore.Release();
                _flushSemaphore.Dispose();
            }
        }
    }
}
