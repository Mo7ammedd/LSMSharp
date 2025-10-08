using System;
using System.Diagnostics;
using System.Threading.Tasks;
using LSMTree;
using LSMTree.Core;

class SimpleBenchmark
{
    static async Task Main()
    {
        var dbPath = "/tmp/benchmark_db";
        System.IO.Directory.CreateDirectory(dbPath);
        foreach (var f in System.IO.Directory.GetFiles(dbPath)) System.IO.File.Delete(f);
        foreach (var d in System.IO.Directory.GetDirectories(dbPath)) System.IO.Directory.Delete(d, true);

        var config = new LSMConfiguration
        {
            MemtableThreshold = 1024 * 1024,
            DataBlockSize = 4096,
            EnableBlockCache = true,
            BlockCacheSize = 64 * 1024 * 1024
        };

        await using var db = await LSMTreeDB.OpenAsync(dbPath, config);
        
        Console.WriteLine("=== LSM-Tree Performance Benchmarks ===\n");
        
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < 10000; i++)
        {
            await db.SetAsync($"key_{i:D6}", System.Text.Encoding.UTF8.GetBytes($"value_{i}"));
        }
        sw.Stop();
        Console.WriteLine($"Sequential Writes: {10000.0 / sw.Elapsed.TotalSeconds:N0} ops/sec");
        
        sw.Restart();
        for (int i = 0; i < 10000; i++)
        {
            await db.GetAsync($"key_{i:D6}");
        }
        sw.Stop();
        Console.WriteLine($"Sequential Reads:  {10000.0 / sw.Elapsed.TotalSeconds:N0} ops/sec");
        
        var tasks = new Task[10];
        sw.Restart();
        for (int t = 0; t < 10; t++)
        {
            int thread = t;
            tasks[t] = Task.Run(async () =>
            {
                for (int i = 0; i < 1000; i++)
                {
                    await db.SetAsync($"concurrent_{thread}_{i}", System.Text.Encoding.UTF8.GetBytes($"val_{i}"));
                }
            });
        }
        await Task.WhenAll(tasks);
        sw.Stop();
        Console.WriteLine($"Concurrent Writes: {10000.0 / sw.Elapsed.TotalSeconds:N0} ops/sec");
        
        var stats = db.GetCacheStats();
        if (stats != null)
        {
            Console.WriteLine($"\nCache Stats:");
            Console.WriteLine($"  Hit Ratio: {stats.Value.HitRatio:P1}");
            Console.WriteLine($"  Hits: {stats.Value.Hits}");
            Console.WriteLine($"  Misses: {stats.Value.Misses}");
        }
    }
}
