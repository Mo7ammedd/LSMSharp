using System;
using System.Collections.Generic;
using System.Threading;
using LSMTree.Core;

namespace LSMTree.SkipList
{
    internal class SkipListNode
    {
        public Entry Entry { get; set; }
        public SkipListNode?[] Next { get; }
        public readonly ReaderWriterLockSlim Lock;

        public SkipListNode(Entry entry, int level)
        {
            Entry = entry;
            Next = new SkipListNode[level];
            Lock = new ReaderWriterLockSlim();
        }

        public SkipListNode(int level) : this(default, level) { }
    }

    public class ConcurrentSkipList : ISkipList
    {
        private const int MaxLevel = 32;
        private const double Probability = 0.5;

        private readonly SkipListNode _head;
        private int _level;
        private int _size;

        [ThreadStatic]
        private static Random? _threadRandom;
        private static Random ThreadRandom => _threadRandom ??= new Random(Guid.NewGuid().GetHashCode());

        public int Size 
        { 
            get => Volatile.Read(ref _size);
        }

        public bool IsEmpty 
        { 
            get => Volatile.Read(ref _size) == 0;
        }

        public ConcurrentSkipList()
        {
            _head = new SkipListNode(MaxLevel);
            _level = 1;
            _size = 0;
        }

        public void Set(Entry entry)
        {
            var update = new SkipListNode[MaxLevel];
            var current = _head;
            int currentLevel = Volatile.Read(ref _level);

            _head.Lock.EnterReadLock();
            try
            {
                for (int i = currentLevel - 1; i >= 0; i--)
                {
                    while (current.Next[i] != null && 
                           string.Compare(current.Next[i]!.Entry.Key, entry.Key, StringComparison.Ordinal) < 0)
                    {
                        current = current.Next[i]!;
                    }
                    update[i] = current;
                }

                current = current.Next[0];

                if (current != null && current.Entry.Key == entry.Key)
                {
                    current.Lock.EnterWriteLock();
                    try
                    {
                        int oldSize = EstimateEntrySize(current.Entry);
                        int newSize = EstimateEntrySize(entry);
                        current.Entry = entry;
                        Interlocked.Add(ref _size, newSize - oldSize);
                    }
                    finally
                    {
                        current.Lock.ExitWriteLock();
                    }
                    return;
                }
            }
            finally
            {
                _head.Lock.ExitReadLock();
            }

            int newLevel = GetRandomLevel();
            
            _head.Lock.EnterWriteLock();
            try
            {
                currentLevel = Volatile.Read(ref _level);
                
                if (newLevel > currentLevel)
                {
                    for (int i = currentLevel; i < newLevel; i++)
                    {
                        update[i] = _head;
                    }
                    Volatile.Write(ref _level, newLevel);
                }

                var newNode = new SkipListNode(entry, newLevel);
                for (int i = 0; i < newLevel; i++)
                {
                    newNode.Next[i] = update[i].Next[i];
                    update[i].Next[i] = newNode;
                }

                Interlocked.Add(ref _size, EstimateEntrySize(entry) + EstimateNodeOverhead(newLevel));
            }
            finally
            {
                _head.Lock.ExitWriteLock();
            }
        }

        public (bool found, Entry entry) Get(string key)
        {
            var current = _head;
            int currentLevel = Volatile.Read(ref _level);

            _head.Lock.EnterReadLock();
            try
            {
                for (int i = currentLevel - 1; i >= 0; i--)
                {
                    while (current.Next[i] != null && 
                           string.Compare(current.Next[i]!.Entry.Key, key, StringComparison.Ordinal) < 0)
                    {
                        current = current.Next[i]!;
                    }
                }

                current = current.Next[0];

                if (current != null && current.Entry.Key == key)
                {
                    current.Lock.EnterReadLock();
                    try
                    {
                        return (true, current.Entry);
                    }
                    finally
                    {
                        current.Lock.ExitReadLock();
                    }
                }

                return (false, default);
            }
            finally
            {
                _head.Lock.ExitReadLock();
            }
        }

        public IEnumerable<Entry> GetAll()
        {
            var entries = new List<Entry>();
            
            _head.Lock.EnterReadLock();
            try
            {
                var current = _head.Next[0];

                while (current != null)
                {
                    current.Lock.EnterReadLock();
                    try
                    {
                        entries.Add(current.Entry);
                    }
                    finally
                    {
                        current.Lock.ExitReadLock();
                    }
                    current = current.Next[0];
                }

                return entries;
            }
            finally
            {
                _head.Lock.ExitReadLock();
            }
        }

        private int GetRandomLevel()
        {
            int level = 1;
            while (level < MaxLevel && ThreadRandom.NextDouble() < Probability)
            {
                level++;
            }
            return level;
        }

        private static int EstimateEntrySize(Entry entry)
        {
            return sizeof(long) + 
                   sizeof(bool) + 
                   (entry.Key?.Length ?? 0) * sizeof(char) + 
                   (entry.Value?.Length ?? 0);
        }

        private static int EstimateNodeOverhead(int level)
        {
            return level * IntPtr.Size;
        }
    }
}
