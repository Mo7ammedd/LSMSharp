using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using LSMTree;

namespace LSMTree.Tests
{
    public class PerformanceTests
    {
        private const int WARMUP_OPERATIONS = 500;
        private const int MEASUREMENT_OPERATIONS = 5000;
        private const int CONCURRENT_OPERATIONS = 5000; // Increased from 1000
        private const int STRESS_OPERATIONS = 25000; // Increased from 5000

        public static async Task RunAllAsync()
        {
            var totalStopwatch = Stopwatch.StartNew();
            Console.WriteLine("LSM-Tree Performance Tests");
            Console.WriteLine("==========================");
            Console.WriteLine($"Started at: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            Console.WriteLine();

            await WarmupTest();
            await SequentialWriteTest();
            await RandomWriteTest();
            await SequentialReadTest();
            await RandomReadTest();
            await ConcurrentWriteTest();
            await ConcurrentReadTest();
            await MixedWorkloadTest();
            await StressTest();

            totalStopwatch.Stop();
            Console.WriteLine();
            Console.WriteLine($"Performance tests completed in {totalStopwatch.ElapsedMilliseconds}ms ({totalStopwatch.Elapsed.TotalSeconds:F2}s)!");
            Console.WriteLine($"Finished at: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        }

        private static async Task WarmupTest()
        {
            Console.WriteLine("1. Warmup Test");
            Console.WriteLine("--------------");
            Console.WriteLine($"Warming up database with {WARMUP_OPERATIONS} operations...");

            var dbPath = Path.Combine(Environment.CurrentDirectory, "test_perf_warmup");
            CleanupDirectory(dbPath);
            await using var db = await LSMTreeDB.OpenAsync(dbPath);
            var stopwatch = Stopwatch.StartNew();
            var tasks = new List<Task>();

            for (int i = 0; i < WARMUP_OPERATIONS; i++)
            {
                var key = $"warmup:{i:D6}";
                var value = GenerateRandomValue(75);
                tasks.Add(db.SetAsync(key, value));
            }

            await Task.WhenAll(tasks);
            stopwatch.Stop();

            Console.WriteLine($"✓ Warmup: {WARMUP_OPERATIONS} operations in {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"  Throughput: {WARMUP_OPERATIONS * 1000.0 / stopwatch.ElapsedMilliseconds:F0} ops/sec");
            Console.WriteLine($"  Average latency: {(double)stopwatch.ElapsedMilliseconds / WARMUP_OPERATIONS:F2}ms per operation");
            Console.WriteLine();
        }

        private static async Task SequentialWriteTest()
        {
            Console.WriteLine("2. Sequential Write Test");
            Console.WriteLine("-----------------------");
            Console.WriteLine($"Testing sequential writes with {MEASUREMENT_OPERATIONS} operations...");

            var dbPath = Path.Combine(Environment.CurrentDirectory, "test_perf_seq_write");
            CleanupDirectory(dbPath);
            await using var db = await LSMTreeDB.OpenAsync(dbPath);
            var stopwatch = Stopwatch.StartNew();
            var tasks = new List<Task>();

            for (int i = 0; i < MEASUREMENT_OPERATIONS; i++)
            {
                var key = $"seq_write:{i:D8}";
                var value = GenerateRandomValue(256);
                tasks.Add(db.SetAsync(key, value));

                if (i > 0 && i % 1000 == 0)
                {
                    Console.WriteLine($"  Progress: {i}/{MEASUREMENT_OPERATIONS} operations ({i * 100.0 / MEASUREMENT_OPERATIONS:F1}%)");
                }
            }

            await Task.WhenAll(tasks);
            stopwatch.Stop();

            var throughput = MEASUREMENT_OPERATIONS * 1000.0 / stopwatch.ElapsedMilliseconds;
            var avgLatency = (double)stopwatch.ElapsedMilliseconds / MEASUREMENT_OPERATIONS;

            Console.WriteLine($"✓ Sequential writes: {MEASUREMENT_OPERATIONS} operations in {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"  Throughput: {throughput:F0} ops/sec");
            Console.WriteLine($"  Average latency: {avgLatency:F3}ms per operation");
            Console.WriteLine($"  Total data written: ~{MEASUREMENT_OPERATIONS * 256 / 1024.0:F1} KB");
            Console.WriteLine();
        }

        private static async Task RandomWriteTest()
        {
            Console.WriteLine("\n3. Random Write Test");
            Console.WriteLine("-------------------");

            var dbPath = Path.Combine(Environment.CurrentDirectory, "test_perf_rand_write");
            CleanupDirectory(dbPath);
            await using var db = await LSMTreeDB.OpenAsync(dbPath);
            var random = new Random(42); // Fixed seed for reproducibility
            var stopwatch = Stopwatch.StartNew();
            var tasks = new List<Task>();

            for (int i = 0; i < MEASUREMENT_OPERATIONS; i++)
            {
                var key = $"random_write:{random.Next(0, MEASUREMENT_OPERATIONS * 2):D8}";
                var value = GenerateRandomValue(256);
                tasks.Add(db.SetAsync(key, value));
            }

            await Task.WhenAll(tasks);
            stopwatch.Stop();

            var throughput = MEASUREMENT_OPERATIONS * 1000.0 / stopwatch.ElapsedMilliseconds;
            Console.WriteLine($"Random writes: {MEASUREMENT_OPERATIONS} operations in {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"Throughput: {throughput:F0} ops/sec");
        }

        private static async Task SequentialReadTest()
        {
            Console.WriteLine("\n4. Sequential Read Test");
            Console.WriteLine("----------------------");

            // Use the same data from sequential write test
            var dbPath = Path.Combine(Environment.CurrentDirectory, "test_perf_seq_write");
            await using var db = await LSMTreeDB.OpenAsync(dbPath);
            var stopwatch = Stopwatch.StartNew();
            var tasks = new List<Task<(bool, byte[])>>();
            int foundCount = 0;

            for (int i = 0; i < MEASUREMENT_OPERATIONS; i++)
            {
                var key = $"seq_write:{i:D6}";
                tasks.Add(db.GetAsync(key));
            }

            var results = await Task.WhenAll(tasks);
            stopwatch.Stop();

            foundCount = results.Count(r => r.Item1);
            var throughput = MEASUREMENT_OPERATIONS * 1000.0 / stopwatch.ElapsedMilliseconds;

            Console.WriteLine($"Sequential reads: {MEASUREMENT_OPERATIONS} operations in {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"Throughput: {throughput:F0} ops/sec");
            Console.WriteLine($"Hit rate: {foundCount * 100.0 / MEASUREMENT_OPERATIONS:F1}% ({foundCount}/{MEASUREMENT_OPERATIONS})");
        }

        private static async Task RandomReadTest()
        {
            Console.WriteLine("\n5. Random Read Test");
            Console.WriteLine("------------------");

            // Use the same data from random write test
            var dbPath = Path.Combine(Environment.CurrentDirectory, "test_perf_rand_write");
            await using var db = await LSMTreeDB.OpenAsync(dbPath);
            var random = new Random(42);
            var stopwatch = Stopwatch.StartNew();
            var tasks = new List<Task<(bool, byte[])>>();

            for (int i = 0; i < MEASUREMENT_OPERATIONS; i++)
            {
                var key = $"random_write:{random.Next(0, MEASUREMENT_OPERATIONS * 2):D8}";
                tasks.Add(db.GetAsync(key));
            }

            var results = await Task.WhenAll(tasks);
            stopwatch.Stop();

            int foundCount = results.Count(r => r.Item1);
            var throughput = MEASUREMENT_OPERATIONS * 1000.0 / stopwatch.ElapsedMilliseconds;

            Console.WriteLine($"Random reads: {MEASUREMENT_OPERATIONS} operations in {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"Throughput: {throughput:F0} ops/sec");
            Console.WriteLine($"Hit rate: {foundCount * 100.0 / MEASUREMENT_OPERATIONS:F1}% ({foundCount}/{MEASUREMENT_OPERATIONS})");
        }

        private static async Task ConcurrentWriteTest()
        {
            Console.WriteLine("\n6. Concurrent Write Test");
            Console.WriteLine("-----------------------");

            var dbPath = Path.Combine(Environment.CurrentDirectory, "test_perf_concurrent_write");
            CleanupDirectory(dbPath);
            await using var db = await LSMTreeDB.OpenAsync(dbPath);
            var stopwatch = Stopwatch.StartNew();
            
            // Process in batches to avoid overwhelming the system
            var batchSize = 1000;
            var totalBatches = CONCURRENT_OPERATIONS / batchSize;

            for (int batch = 0; batch < totalBatches; batch++)
            {
                var tasks = new List<Task>();
                
                for (int i = 0; i < batchSize; i++)
                {
                    var recordId = batch * batchSize + i;
                    var key = $"concurrent_write:{recordId:D8}";
                    var value = GenerateRandomValue(128);
                    tasks.Add(db.SetAsync(key, value));
                }

                await Task.WhenAll(tasks);
            }

            stopwatch.Stop();

            var throughput = CONCURRENT_OPERATIONS * 1000.0 / stopwatch.ElapsedMilliseconds;
            Console.WriteLine($"Concurrent writes: {CONCURRENT_OPERATIONS} operations in {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"Throughput: {throughput:F0} ops/sec");
        }

        private static async Task ConcurrentReadTest()
        {
            Console.WriteLine("\n7. Concurrent Read Test");
            Console.WriteLine("----------------------");

            // Use the same data from concurrent write test
            var dbPath = Path.Combine(Environment.CurrentDirectory, "test_perf_concurrent_write");
            await using var db = await LSMTreeDB.OpenAsync(dbPath);
            
            // Wait a bit to ensure all writes are flushed and available for reading
            await Task.Delay(100);
            await db.FlushAsync(); // Ensure all data is flushed to storage
            
            var random = new Random(42);
            var stopwatch = Stopwatch.StartNew();
            var tasks = new List<Task<(bool, byte[])>>();

            for (int i = 0; i < CONCURRENT_OPERATIONS; i++)
            {
                var key = $"concurrent_write:{i:D8}"; // Use sequential keys that we know exist
                tasks.Add(db.GetAsync(key));
            }

            var results = await Task.WhenAll(tasks);
            stopwatch.Stop();

            int foundCount = results.Count(r => r.Item1);
            var throughput = CONCURRENT_OPERATIONS * 1000.0 / stopwatch.ElapsedMilliseconds;

            Console.WriteLine($"Concurrent reads: {CONCURRENT_OPERATIONS} operations in {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"Throughput: {throughput:F0} ops/sec");
            Console.WriteLine($"Hit rate: {foundCount * 100.0 / CONCURRENT_OPERATIONS:F1}% ({foundCount}/{CONCURRENT_OPERATIONS})");
        }

        private static async Task MixedWorkloadTest()
        {
            Console.WriteLine("\n8. Mixed Workload Test (70% reads, 30% writes)");
            Console.WriteLine("----------------------------------------------");

            var dbPath = Path.Combine(Environment.CurrentDirectory, "test_perf_mixed");
            CleanupDirectory(dbPath);
            await using var db = await LSMTreeDB.OpenAsync(dbPath);
            var random = new Random(42);
            var stopwatch = Stopwatch.StartNew();
            var tasks = new List<Task>();

            for (int i = 0; i < MEASUREMENT_OPERATIONS; i++)
            {
                if (random.NextDouble() < 0.3) // 30% writes
                {
                    var key = $"mixed:{i:D8}";
                    var value = GenerateRandomValue(200);
                    tasks.Add(db.SetAsync(key, value));
                }
                else // 70% reads
                {
                    var key = $"mixed:{random.Next(0, i + 1):D8}";
                    tasks.Add(db.GetAsync(key).ContinueWith(_ => { })); // Convert to Task
                }
            }

            await Task.WhenAll(tasks);
            stopwatch.Stop();

            var throughput = MEASUREMENT_OPERATIONS * 1000.0 / stopwatch.ElapsedMilliseconds;
            Console.WriteLine($"Mixed workload: {MEASUREMENT_OPERATIONS} operations in {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"Throughput: {throughput:F0} ops/sec");
        }

        private static async Task StressTest()
        {
            Console.WriteLine("\n9. Stress Test");
            Console.WriteLine("--------------");

            var dbPath = Path.Combine(Environment.CurrentDirectory, "test_perf_stress");
            CleanupDirectory(dbPath);
            await using var db = await LSMTreeDB.OpenAsync(dbPath);
            var random = new Random(42);
            var stopwatch = Stopwatch.StartNew();
            var writeTasks = new List<Task>();
            var readTasks = new List<Task>();

            // Generate stress load with writes
            for (int i = 0; i < STRESS_OPERATIONS; i++)
            {
                var key = $"stress:{i:D8}";
                var value = GenerateRandomValue(512);
                writeTasks.Add(db.SetAsync(key, value));
            }

            // Concurrent reads during writes
            for (int i = 0; i < STRESS_OPERATIONS / 2; i++)
            {
                var key = $"stress:{random.Next(0, i + 1):D8}";
                readTasks.Add(db.GetAsync(key).ContinueWith(_ => { }));
            }

            await Task.WhenAll(writeTasks.Concat(readTasks));
            stopwatch.Stop();

            var totalOps = STRESS_OPERATIONS + (STRESS_OPERATIONS / 2);
            var throughput = totalOps * 1000.0 / stopwatch.ElapsedMilliseconds;

            Console.WriteLine($"Stress test: {totalOps} operations in {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"Throughput: {throughput:F0} ops/sec");
            Console.WriteLine($"Memory usage: {GC.GetTotalMemory(false) / 1024 / 1024:F1} MB");

            // Force compaction after stress test
            await db.CompactAsync();
            Console.WriteLine("Compaction completed after stress test");
        }

        private static byte[] GenerateRandomValue(int size)
        {
            var random = new Random();
            var value = new byte[size];
            random.NextBytes(value);
            return value;
        }

        private static void CleanupDirectory(string path)
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, true);
            }
        }
    }
}
