using System;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using LSMTree;
using LSMTree.Core;

namespace LSMTree.Tests
{
    public class MillionOpTest
    {
        public static async Task RunAsync()
        {
            Console.WriteLine("\n╔══════════════════════════════════════╗");
            Console.WriteLine("║    1 Million Operations Test         ║");
            Console.WriteLine("╚══════════════════════════════════════╝\n");

            var dbPath = "test_million_ops_db";
            if (System.IO.Directory.Exists(dbPath))
            {
                System.IO.Directory.Delete(dbPath, true);
            }

            var config = new LSMConfiguration
            {
                MemtableThreshold = 4 * 1024 * 1024,
                DataBlockSize = 4096,
                EnableBlockCache = true,
                BlockCacheSize = 128 * 1024 * 1024,
                Level0CompactionTrigger = 8
            };

            await using var db = await LSMTreeDB.OpenAsync(dbPath, config);

            Console.WriteLine("📝 Writing 1,000,000 operations...\n");

            var sw = Stopwatch.StartNew();
            var checkpoints = new[] { 100000, 250000, 500000, 750000, 1000000 };
            var lastCheckpoint = 0;

            for (int i = 0; i < 1000000; i++)
            {
                var key = $"key_{i:D10}";
                var value = Encoding.UTF8.GetBytes($"value_{i}_data");
                await db.SetAsync(key, value);

                if (Array.IndexOf(checkpoints, i + 1) >= 0)
                {
                    var elapsed = sw.Elapsed.TotalSeconds;
                    var opsCount = i + 1 - lastCheckpoint;
                    var throughput = opsCount / (elapsed - (lastCheckpoint / (i + 1.0) * elapsed));
                    Console.WriteLine($"  ✓ {i + 1:N0} operations | {elapsed:F1}s elapsed | {(i + 1) / elapsed:N0} ops/sec avg");
                    lastCheckpoint = i + 1;
                }
            }

            sw.Stop();
            var totalSeconds = sw.Elapsed.TotalSeconds;
            var totalThroughput = 1000000 / totalSeconds;

            Console.WriteLine($"\n✅ Write Phase Complete!");
            Console.WriteLine($"   Total Time: {totalSeconds:F2}s");
            Console.WriteLine($"   Throughput: {totalThroughput:N0} ops/sec");
            Console.WriteLine($"   Avg Latency: {totalSeconds * 1000 / 1000000:F3}ms per operation\n");

            Console.WriteLine("💾 Flushing to disk...");
            var flushSw = Stopwatch.StartNew();
            await db.FlushAsync();
            flushSw.Stop();
            Console.WriteLine($"   Flush Time: {flushSw.ElapsedMilliseconds}ms\n");

            Console.WriteLine("🔍 Random Read Test (10,000 samples)...");
            sw.Restart();
            var random = new Random(42);
            int foundCount = 0;

            for (int i = 0; i < 10000; i++)
            {
                var idx = random.Next(1000000);
                var key = $"key_{idx:D10}";
                var (found, value) = await db.GetAsync(key);
                if (found) foundCount++;
            }

            sw.Stop();
            var readThroughput = 10000 / sw.Elapsed.TotalSeconds;

            Console.WriteLine($"   Read Time: {sw.ElapsedMilliseconds}ms");
            Console.WriteLine($"   Throughput: {readThroughput:N0} ops/sec");
            Console.WriteLine($"   Hit Rate: {foundCount * 100.0 / 10000:F1}%\n");

            var stats = db.GetCacheStats();
            if (stats.HasValue)
            {
                Console.WriteLine("📊 Cache Statistics:");
                Console.WriteLine($"   Hit Ratio: {stats.Value.HitRatio:P1}");
                Console.WriteLine($"   Hits: {stats.Value.Hits:N0}");
                Console.WriteLine($"   Misses: {stats.Value.Misses:N0}");
                Console.WriteLine($"   Size: {stats.Value.Size / (1024.0 * 1024.0):F2} MB");
            }

            Console.WriteLine($"\n🎯 Test Complete!");
            Console.WriteLine($"   Database: {dbPath}");
        }
    }
}
