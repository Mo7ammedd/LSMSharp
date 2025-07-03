using System.Text;
using LSMTree;

namespace LSMTree
{    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("LSM-Tree Storage Engine Demo");
            Console.WriteLine("============================");

            // Create database directory
            var dbPath = Path.Combine(Environment.CurrentDirectory, "lsmdb");
            if (Directory.Exists(dbPath))
            {
                Directory.Delete(dbPath, true);
            }

            // Open LSM-Tree database
            using var db = await LSMTreeDB.OpenAsync(dbPath);

            // Demo 1: Basic Operations
            Console.WriteLine("\n1. Basic Operations");
            Console.WriteLine("-------------------");

            // Insert some key-value pairs
            await db.SetAsync("user:1", Encoding.UTF8.GetBytes("Alice"));
            await db.SetAsync("user:2", Encoding.UTF8.GetBytes("Bob"));
            await db.SetAsync("user:3", Encoding.UTF8.GetBytes("Charlie"));
            
            Console.WriteLine("Inserted 3 users");

            // Read values
            var (found1, value1) = await db.GetAsync("user:1");
            Console.WriteLine($"user:1 = {(found1 ? Encoding.UTF8.GetString(value1) : "Not found")}");

            var (found2, value2) = await db.GetAsync("user:2");
            Console.WriteLine($"user:2 = {(found2 ? Encoding.UTF8.GetString(value2) : "Not found")}");

            // Demo 2: Updates
            Console.WriteLine("\n2. Updates");
            Console.WriteLine("----------");
            
            await db.SetAsync("user:1", Encoding.UTF8.GetBytes("Alice Updated"));
            var (foundUpdated, valueUpdated) = await db.GetAsync("user:1");
            Console.WriteLine($"user:1 (updated) = {(foundUpdated ? Encoding.UTF8.GetString(valueUpdated) : "Not found")}");

            // Demo 3: Deletions
            Console.WriteLine("\n3. Deletions");
            Console.WriteLine("------------");
            
            await db.DeleteAsync("user:2");
            var (foundDeleted, _) = await db.GetAsync("user:2");
            Console.WriteLine($"user:2 (after deletion) = {(foundDeleted ? "Found" : "Not found")}");

            // Demo 4: Bulk Operations to Trigger Flush
            Console.WriteLine("\n4. Bulk Operations (Triggering Flush)");
            Console.WriteLine("-------------------------------------");

            var random = new Random();
            var tasks = new List<Task>();

            for (int i = 0; i < 1000; i++)
            {
                var key = $"bulk:{i:D4}";
                var value = Encoding.UTF8.GetBytes($"Value {i} - {random.Next(1000, 9999)}");
                tasks.Add(db.SetAsync(key, value));
            }

            await Task.WhenAll(tasks);
            Console.WriteLine("Inserted 1000 bulk records");

            // Force flush to disk
            await db.FlushAsync();
            Console.WriteLine("Flushed memtable to SSTables");

            // Demo 5: Read some bulk data
            Console.WriteLine("\n5. Reading Bulk Data");
            Console.WriteLine("--------------------");

            var bulkReadTasks = new List<Task<(bool, byte[])>>();
            for (int i = 0; i < 10; i++)
            {
                var key = $"bulk:{i:D4}";
                bulkReadTasks.Add(db.GetAsync(key));
            }

            var results = await Task.WhenAll(bulkReadTasks);
            for (int i = 0; i < results.Length; i++)
            {
                var (found, value) = results[i];
                Console.WriteLine($"bulk:{i:D4} = {(found ? Encoding.UTF8.GetString(value) : "Not found")}");
            }

            // Demo 6: Compaction
            Console.WriteLine("\n6. Compaction");
            Console.WriteLine("-------------");
            
            await db.CompactAsync();
            Console.WriteLine("Triggered compaction");

            // Demo 7: Performance Test
            Console.WriteLine("\n7. Performance Test");
            Console.WriteLine("-------------------");

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var writeTasks = new List<Task>();

            for (int i = 0; i < 10000; i++)
            {
                var key = $"perf:{i:D6}";
                var value = Encoding.UTF8.GetBytes($"Performance test value {i}");
                writeTasks.Add(db.SetAsync(key, value));
            }

            await Task.WhenAll(writeTasks);
            stopwatch.Stop();

            Console.WriteLine($"Wrote 10,000 records in {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"Write throughput: {10000.0 / stopwatch.Elapsed.TotalSeconds:F0} ops/sec");

            // Read performance test
            stopwatch.Restart();
            var readTasks = new List<Task<(bool, byte[])>>();

            for (int i = 0; i < 1000; i++)
            {
                var key = $"perf:{random.Next(0, 10000):D6}";
                readTasks.Add(db.GetAsync(key));
            }

            var readResults = await Task.WhenAll(readTasks);
            stopwatch.Stop();

            int foundCount = readResults.Count(r => r.Item1);
            Console.WriteLine($"Read 1,000 random records in {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"Read throughput: {1000.0 / stopwatch.Elapsed.TotalSeconds:F0} ops/sec");
            Console.WriteLine($"Found {foundCount}/1000 records");

            Console.WriteLine("\nDemo completed successfully!");
            Console.WriteLine("Check the 'lsmdb' directory for generated files.");
        }
    }
}
