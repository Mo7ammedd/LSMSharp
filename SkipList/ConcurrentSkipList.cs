using System;
using System.Collections.Generic;
using LSMTree.Core;

namespace LSMTree.SkipList
{
    internal class SkipListNode
    {
        public Entry Entry { get; set; }
        public SkipListNode?[] Next { get; }

        public SkipListNode(Entry entry, int level)
        {
            Entry = entry;
            Next = new SkipListNode[level];
        }

        public SkipListNode(int level) : this(default, level) { }
    }

    public class ConcurrentSkipList : ISkipList
    {
        private const int MaxLevel = 32;
        private const double Probability = 0.5;

        private readonly SkipListNode _head;
        private readonly Random _random;
        private readonly object _lock = new object();
        
        private int _level;
        private int _size;

        public int Size 
        { 
            get 
            { 
                lock (_lock) 
                { 
                    return _size; 
                } 
            } 
        }

        public bool IsEmpty 
        { 
            get 
            { 
                lock (_lock) 
                { 
                    return _size == 0; 
                } 
            } 
        }

        public ConcurrentSkipList()
        {
            _head = new SkipListNode(MaxLevel);
            _random = new Random();
            _level = 1;
            _size = 0;
        }

        public void Set(Entry entry)
        {
            lock (_lock)
            {
                var update = new SkipListNode[MaxLevel];
                var current = _head;

                // Find position to insert/update
                for (int i = _level - 1; i >= 0; i--)
                {
                    while (current.Next[i] != null && 
                           string.Compare(current.Next[i]!.Entry.Key, entry.Key, StringComparison.Ordinal) < 0)
                    {
                        current = current.Next[i]!;
                    }
                    update[i] = current;
                }

                current = current.Next[0];

                // Update existing entry
                if (current != null && current.Entry.Key == entry.Key)
                {
                    _size -= EstimateEntrySize(current.Entry);
                    current.Entry = entry;
                    _size += EstimateEntrySize(entry);
                    return;
                }

                // Insert new entry
                int newLevel = GetRandomLevel();
                if (newLevel > _level)
                {
                    for (int i = _level; i < newLevel; i++)
                    {
                        update[i] = _head;
                    }
                    _level = newLevel;
                }

                var newNode = new SkipListNode(entry, newLevel);
                for (int i = 0; i < newLevel; i++)
                {
                    newNode.Next[i] = update[i].Next[i];
                    update[i].Next[i] = newNode;
                }

                _size += EstimateEntrySize(entry) + EstimateNodeOverhead(newLevel);
            }
        }

        public (bool found, Entry entry) Get(string key)
        {
            lock (_lock)
            {
                var current = _head;

                for (int i = _level - 1; i >= 0; i--)
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
                    return (true, current.Entry);
                }

                return (false, default);
            }
        }

        public IEnumerable<Entry> GetAll()
        {
            lock (_lock)
            {
                var entries = new List<Entry>();
                var current = _head.Next[0];

                while (current != null)
                {
                    entries.Add(current.Entry);
                    current = current.Next[0];
                }

                return entries;
            }
        }

        private int GetRandomLevel()
        {
            int level = 1;
            while (level < MaxLevel && _random.NextDouble() < Probability)
            {
                level++;
            }
            return level;
        }

        private static int EstimateEntrySize(Entry entry)
        {
            return sizeof(long) + // timestamp
                   sizeof(bool) + // tombstone
                   (entry.Key?.Length ?? 0) * sizeof(char) + // key
                   (entry.Value?.Length ?? 0); // value
        }

        private static int EstimateNodeOverhead(int level)
        {
            return level * IntPtr.Size; // next pointers
        }
    }
}
