using System;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text;
using LSMTree.Core;

namespace LSMTree.WAL
{
    public class WriteAheadLog : IWriteAheadLog
    {
        private readonly string _filePath;
        private readonly FileStream _fileStream;
        private readonly BinaryWriter _writer;
        private readonly object _writeLock = new object();
        private bool _disposed = false;

        public WriteAheadLog(string filePath)
        {
            _filePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
            
            // Ensure directory exists
            var directory = Path.GetDirectoryName(_filePath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            _fileStream = new FileStream(_filePath, FileMode.Append, FileAccess.Write, FileShare.Read);
            _writer = new BinaryWriter(_fileStream, Encoding.UTF8, leaveOpen: true);
        }

        public Task WriteAsync(params Entry[] entries)
        {
            if (entries == null || entries.Length == 0)
                return Task.CompletedTask;

            lock (_writeLock)
            {
                if (_disposed)
                    throw new ObjectDisposedException(nameof(WriteAheadLog));

                foreach (var entry in entries)
                {
                    WriteEntry(entry);
                }
                _writer.Flush();
                _fileStream.Flush(true); // Force sync to disk
            }
            
            return Task.CompletedTask;
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
                    // End of file reached
                    break;
                }
                catch (Exception)
                {
                    // Corrupted entry, stop reading
                    break;
                }
            }

            return Task.FromResult<IEnumerable<Entry>>(entries);
        }

        public Task DeleteAsync()
        {
            if (_disposed)
                return Task.CompletedTask;

            lock (_writeLock)
            {
                if (!_disposed)
                {
                    _writer?.Close();
                    _fileStream?.Close();
                    _disposed = true;
                    
                    if (File.Exists(_filePath))
                    {
                        File.Delete(_filePath);
                    }
                }
            }
            
            return Task.CompletedTask;
        }

        public Task SyncAsync()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(WriteAheadLog));

            lock (_writeLock)
            {
                _writer.Flush();
                _fileStream.Flush(true);
            }
            
            return Task.CompletedTask;
        }

        private void WriteEntry(Entry entry)
        {
            // Format: [length][key][value_length][value][tombstone][timestamp]
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
            // Read key
            int keyLength = reader.ReadInt32();
            var keyBytes = reader.ReadBytes(keyLength);
            string key = Encoding.UTF8.GetString(keyBytes);

            // Read value
            int valueLength = reader.ReadInt32();
            var value = reader.ReadBytes(valueLength);

            // Read tombstone and timestamp
            bool tombstone = reader.ReadBoolean();
            long timestamp = reader.ReadInt64();

            return new Entry(key, value, tombstone, timestamp);
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            lock (_writeLock)
            {
                if (!_disposed)
                {
                    _writer?.Dispose();
                    _fileStream?.Dispose();
                    _disposed = true;
                }
            }
        }
    }
}
