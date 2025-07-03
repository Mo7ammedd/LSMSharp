using System;
using System.Collections.Generic;
using System.Linq;
using LSMTree.Core;

namespace LSMTree.Utils
{
    internal class MergeElement : IComparable<MergeElement>
    {
        public Entry Entry { get; }
        public int ListIndex { get; }
        public int ElementIndex { get; }

        public MergeElement(Entry entry, int listIndex, int elementIndex)
        {
            Entry = entry;
            ListIndex = listIndex;
            ElementIndex = elementIndex;
        }

        public int CompareTo(MergeElement? other)
        {
            if (other == null) return 1;

            int keyComparison = string.Compare(Entry.Key, other.Entry.Key, StringComparison.Ordinal);
            if (keyComparison != 0)
                return keyComparison;

            // If keys are equal, prioritize by list index (older lists first for deduplication)
            return ListIndex.CompareTo(other.ListIndex);
        }
    }

    public static class KWayMerge
    {
        /// <param name="lists">Sorted lists to merge (should be ordered from oldest to newest)</param>
        /// <returns>Merged and deduplicated list of entries</returns>
        public static List<Entry> Merge(params IEnumerable<Entry>[] lists)
        {
            if (lists == null || lists.Length == 0)
                return new List<Entry>();

            // Convert to arrays for efficient indexing
            var arrays = lists.Select(list => list.ToArray()).ToArray();
            
            // Priority queue for merge
            var heap = new SortedSet<MergeElement>();
            var indices = new int[arrays.Length];

            // Initialize heap with first element from each non-empty list
            for (int i = 0; i < arrays.Length; i++)
            {
                if (arrays[i].Length > 0)
                {
                    heap.Add(new MergeElement(arrays[i][0], i, 0));
                }
            }

            var result = new Dictionary<string, Entry>();

            // Merge process
            while (heap.Count > 0)
            {
                var min = heap.Min;
                if (min != null)
                {
                    heap.Remove(min);

                    // Add to result (newer entries will overwrite older ones due to key equality)
                    result[min.Entry.Key] = min.Entry;

                    // Add next element from the same list
                    int nextIndex = min.ElementIndex + 1;
                    if (nextIndex < arrays[min.ListIndex].Length)
                    {
                        heap.Add(new MergeElement(
                            arrays[min.ListIndex][nextIndex], 
                            min.ListIndex, 
                            nextIndex));
                    }
                }
            }

            // Filter out tombstones and return sorted result
            var finalResult = result.Values
                .Where(entry => !entry.Tombstone)
                .OrderBy(entry => entry.Key, StringComparer.Ordinal)
                .ToList();

            return finalResult;
        }

        public static List<Entry> Merge(IComparer<Entry> comparer, params IEnumerable<Entry>[] lists)
        {
            if (lists == null || lists.Length == 0)
                return new List<Entry>();

            var arrays = lists.Select(list => list.ToArray()).ToArray();
            var heap = new PriorityQueue<MergeElement, Entry>(
                Comparer<Entry>.Create((x, y) => comparer.Compare(x, y)));

            // Initialize heap
            for (int i = 0; i < arrays.Length; i++)
            {
                if (arrays[i].Length > 0)
                {
                    var element = new MergeElement(arrays[i][0], i, 0);
                    heap.Enqueue(element, arrays[i][0]);
                }
            }

            var result = new Dictionary<string, Entry>();

            // Merge process
            while (heap.Count > 0)
            {
                var min = heap.Dequeue();
                result[min.Entry.Key] = min.Entry;

                // Add next element from the same list
                int nextIndex = min.ElementIndex + 1;
                if (nextIndex < arrays[min.ListIndex].Length)
                {
                    var nextElement = new MergeElement(
                        arrays[min.ListIndex][nextIndex], 
                        min.ListIndex, 
                        nextIndex);
                    heap.Enqueue(nextElement, arrays[min.ListIndex][nextIndex]);
                }
            }

            return result.Values
                .Where(entry => !entry.Tombstone)
                .OrderBy(entry => entry, comparer)
                .ToList();
        }

        public static List<Entry> MergeTwo(IEnumerable<Entry> list1, IEnumerable<Entry> list2)
        {
            return Merge(list1, list2);
        }

        public static List<Entry> MergeWithConflictResolution(
            Func<Entry, Entry, Entry> conflictResolver, 
            params IEnumerable<Entry>[] lists)
        {
            if (lists == null || lists.Length == 0)
                return new List<Entry>();

            var result = new Dictionary<string, Entry>();

            foreach (var list in lists)
            {
                foreach (var entry in list)
                {
                    if (result.TryGetValue(entry.Key, out var existing))
                    {
                        result[entry.Key] = conflictResolver(existing, entry);
                    }
                    else
                    {
                        result[entry.Key] = entry;
                    }
                }
            }

            return result.Values
                .Where(entry => !entry.Tombstone)
                .OrderBy(entry => entry.Key, StringComparer.Ordinal)
                .ToList();
        }
    }
}
