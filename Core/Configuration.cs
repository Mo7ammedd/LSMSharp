using System;
using LSMTree.Core;

namespace LSMTree.Core
{
    public class LSMConfiguration
    {
        public int MemtableThreshold { get; set; } = 1024 * 1024;
        public int DataBlockSize { get; set; } = 4096;
        public double BloomFilterFalsePositiveRate { get; set; } = 0.01;
        public int CompactionThreads { get; set; } = 1;
        public CompressionType CompressionType { get; set; } = CompressionType.GZip;
        public TimeSpan FlushInterval { get; set; } = TimeSpan.FromSeconds(30);
        public long BlockCacheSize { get; set; } = 64 * 1024 * 1024;
        public bool EnableBlockCache { get; set; } = true;
        public int MaxLevels { get; set; } = 7;
        public int Level0CompactionTrigger { get; set; } = 4;
        public double CompactionRatio { get; set; } = 10.0;

        public static LSMConfiguration Default => new();

        public void Validate()
        {
            if (MemtableThreshold <= 0)
                throw new ArgumentException("Memtable threshold must be positive");
            
            if (DataBlockSize <= 0)
                throw new ArgumentException("Data block size must be positive");
            
            if (BloomFilterFalsePositiveRate <= 0 || BloomFilterFalsePositiveRate >= 1)
                throw new ArgumentException("Bloom filter false positive rate must be between 0 and 1");
            
            if (CompactionThreads <= 0)
                throw new ArgumentException("Compaction threads must be positive");
            
            if (BlockCacheSize < 0)
                throw new ArgumentException("Block cache size cannot be negative");
            
            if (MaxLevels <= 0)
                throw new ArgumentException("Max levels must be positive");
            
            if (Level0CompactionTrigger <= 0)
                throw new ArgumentException("Level 0 compaction trigger must be positive");
            
            if (CompactionRatio <= 1.0)
                throw new ArgumentException("Compaction ratio must be greater than 1.0");
        }
    }
}
