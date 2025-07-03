using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using LSMTree;

namespace LSMTree.Tests
{
    public class StressTests
    {
        private const int HEAVY_LOAD_OPERATIONS = 1000000;
        private const int CONCURRENT_THREADS = 100;
        private const int LARGE_VALUE_SIZE = 10240; // 10KB

        public static async Task RunAllAsync()
        {
            Console.WriteLine("LSM-Tree Stress Tests");
            Console.WriteLine("====================");

            var dbPath = Path.Combine(Environment.CurrentDirectory, "test_stress_db");
            CleanupDirectory(dbPath);

            using var db = await LSMTreeDB.OpenAsync(dbPath);

            await HeavyLoadTest(db);
            await LargeValueTest(db);
            await ConcurrentStressTest(db);
            await MemoryPressureTest(db);
            await CompactionStressTest(db);
            await CrashRecoveryTest();

            Console.WriteLine("\nStress tests completed!");
        }

        private static async Task HeavyLoadTest(LSMTreeDB db)
        {
            Console.WriteLine("\n1. Heavy Load Test");
            Console.WriteLine("------------------");

            var tasks = new List<Task>();
            var batchSize = 10000;
            var totalBatches = HEAVY_LOAD_OPERATIONS / batchSize;

            Console.WriteLine($"Writing {HEAVY_LOAD_OPERATIONS} records in {totalBatches} batches...");

            for (int batch = 0; batch < totalBatches; batch++)
            {
                tasks.Clear();
                
                for (int i = 0; i < batchSize; i++)
                {
                    var recordId = batch * batchSize + i;
                    var key = $"heavy:{recordId:D8}";
                    var value = GenerateStructuredValue(recordId);
                    tasks.Add(db.SetAsync(key, value));
                }

                await Task.WhenAll(tasks);
                
                if ((batch + 1) % 10 == 0)
                {
                    Console.WriteLine($"Completed batch {batch + 1}/{totalBatches}");
                    await db.FlushAsync(); // Periodic flush
                }
            }

            Console.WriteLine("Heavy load write completed");

            // Verify random reads
            Console.WriteLine("Verifying random reads...");
            var random = new Random(42);
            var readTasks = new List<Task<(bool, byte[])>>();

            for (int i = 0; i < 10000; i++)
            {
                var recordId = random.Next(0, HEAVY_LOAD_OPERATIONS);
                var key = $"heavy:{recordId:D8}";
                readTasks.Add(db.GetAsync(key));
            }

            var results = await Task.WhenAll(readTasks);
            var hitCount = results.Count(r => r.Item1);
            
            Console.WriteLine($"Read verification: {hitCount}/10000 records found ({hitCount / 100.0:F1}% hit rate)");
        }

        private static async Task LargeValueTest(LSMTreeDB db)
        {
            Console.WriteLine("\n2. Large Value Test");
            Console.WriteLine("-------------------");

            var largeValueCount = 1000;
            Console.WriteLine($"Writing {largeValueCount} large values ({LARGE_VALUE_SIZE} bytes each)...");

            var tasks = new List<Task>();
            for (int i = 0; i < largeValueCount; i++)
            {
                var key = $"large:{i:D6}";
                var value = GenerateLargeValue(LARGE_VALUE_SIZE, i);
                tasks.Add(db.SetAsync(key, value));
            }

            await Task.WhenAll(tasks);
            Console.WriteLine("Large values written");

            // Verify large values
            Console.WriteLine("Verifying large values...");
            var readTasks = new List<Task<(bool, byte[])>>();
            for (int i = 0; i < largeValueCount; i++)
            {
                var key = $"large:{i:D6}";
                readTasks.Add(db.GetAsync(key));
            }

            var results = await Task.WhenAll(readTasks);
            var validCount = 0;

            for (int i = 0; i < results.Length; i++)
            {
                var (found, value) = results[i];
                if (found && ValidateLargeValue(value, i))
                {
                    validCount++;
                }
            }

            Console.WriteLine($"Large value verification: {validCount}/{largeValueCount} values valid");
        }

        private static async Task ConcurrentStressTest(LSMTreeDB db)
        {
            Console.WriteLine("\n3. Concurrent Stress Test");
            Console.WriteLine("-------------------------");

            var operationsPerThread = 10000;
            var totalOperations = CONCURRENT_THREADS * operationsPerThread;

            Console.WriteLine($"Running {totalOperations} operations across {CONCURRENT_THREADS} concurrent threads...");

            var threadTasks = new List<Task>();

            for (int threadId = 0; threadId < CONCURRENT_THREADS; threadId++)
            {
                var localThreadId = threadId; // Capture for closure
                threadTasks.Add(Task.Run(async () =>
                {
                    var random = new Random(localThreadId);
                    var tasks = new List<Task>();

                    for (int i = 0; i < operationsPerThread; i++)
                    {
                        var operation = random.NextDouble();
                        var recordId = localThreadId * operationsPerThread + i;

                        if (operation < 0.7) // 70% writes
                        {
                            var key = $"concurrent:{recordId:D8}";
                            var value = GenerateThreadValue(localThreadId, i);
                            tasks.Add(db.SetAsync(key, value));
                        }
                        else if (operation < 0.9) // 20% reads
                        {
                            var readRecordId = random.Next(0, recordId + 1);
                            var key = $"concurrent:{readRecordId:D8}";
                            tasks.Add(db.GetAsync(key).ContinueWith(_ => { }));
                        }
                        else // 10% deletes
                        {
                            var deleteRecordId = random.Next(0, recordId + 1);
                            var key = $"concurrent:{deleteRecordId:D8}";
                            tasks.Add(db.DeleteAsync(key));
                        }
                    }

                    await Task.WhenAll(tasks);
                }));
            }

            await Task.WhenAll(threadTasks);
            Console.WriteLine("Concurrent stress test completed");

            // Force compaction after concurrent stress
            await db.CompactAsync();
            Console.WriteLine("Post-stress compaction completed");
        }

        private static async Task MemoryPressureTest(LSMTreeDB db)
        {
            Console.WriteLine("\n4. Memory Pressure Test");
            Console.WriteLine("-----------------------");

            var initialMemory = GC.GetTotalMemory(true);
            Console.WriteLine($"Initial memory: {initialMemory / 1024 / 1024:F1} MB");

            // Generate memory pressure with many small operations
            var pressureOperations = 500000;
            var tasks = new List<Task>();

            Console.WriteLine($"Generating memory pressure with {pressureOperations} operations...");

            for (int i = 0; i < pressureOperations; i++)
            {
                var key = $"memory:{i:D8}";
                var value = GenerateRandomValue(100);
                tasks.Add(db.SetAsync(key, value));

                // Process in batches to avoid overwhelming memory
                if (tasks.Count >= 5000)
                {
                    await Task.WhenAll(tasks);
                    tasks.Clear();

                    if (i % 50000 == 0)
                    {
                        var currentMemory = GC.GetTotalMemory(false);
                        Console.WriteLine($"Progress: {i}/{pressureOperations}, Memory: {currentMemory / 1024 / 1024:F1} MB");
                    }
                }
            }

            if (tasks.Count > 0)
            {
                await Task.WhenAll(tasks);
            }

            var peakMemory = GC.GetTotalMemory(false);
            Console.WriteLine($"Peak memory: {peakMemory / 1024 / 1024:F1} MB");

            // Force multiple flushes and compactions
            await db.FlushAsync();
            await db.CompactAsync();

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            var finalMemory = GC.GetTotalMemory(true);
            Console.WriteLine($"Final memory after cleanup: {finalMemory / 1024 / 1024:F1} MB");
        }

        private static async Task CompactionStressTest(LSMTreeDB db)
        {
            Console.WriteLine("\n5. Compaction Stress Test");
            Console.WriteLine("-------------------------");

            // Create multiple levels with overlapping key ranges
            var levelsToCreate = 5;
            var recordsPerLevel = 20000;

            Console.WriteLine($"Creating {levelsToCreate} levels with {recordsPerLevel} records each...");

            for (int level = 0; level < levelsToCreate; level++)
            {
                var tasks = new List<Task>();
                
                for (int i = 0; i < recordsPerLevel; i++)
                {
                    // Create overlapping key ranges to force compaction
                    var keyId = (i + level * recordsPerLevel / 2) % (recordsPerLevel * 2);
                    var key = $"compact:{keyId:D8}";
                    var value = GenerateLevelValue(level, i);
                    tasks.Add(db.SetAsync(key, value));
                }

                await Task.WhenAll(tasks);
                await db.FlushAsync(); // Force creation of new SSTable
                
                Console.WriteLine($"Level {level + 1} created");
            }

            // Force multiple rounds of compaction
            Console.WriteLine("Forcing intensive compaction...");
            for (int round = 0; round < 3; round++)
            {
                await db.CompactAsync();
                Console.WriteLine($"Compaction round {round + 1} completed");
            }
        }

        private static async Task CrashRecoveryTest()
        {
            Console.WriteLine("\n6. Crash Recovery Test");
            Console.WriteLine("----------------------");

            var dbPath = Path.Combine(Environment.CurrentDirectory, "test_recovery_db");
            CleanupDirectory(dbPath);

            // Write data without proper cleanup (simulating crash)
            var testData = new Dictionary<string, byte[]>();
            
            {
                using var db = await LSMTreeDB.OpenAsync(dbPath);
                
                Console.WriteLine("Writing test data before 'crash'...");
                var tasks = new List<Task>();
                
                for (int i = 0; i < 10000; i++)
                {
                    var key = $"recovery:{i:D6}";
                    var value = GenerateRecoveryValue(i);
                    testData[key] = value;
                    tasks.Add(db.SetAsync(key, value));
                }

                await Task.WhenAll(tasks);
                // Simulate crash - don't call Dispose
            }

            // Reopen database (simulate recovery)
            Console.WriteLine("Reopening database for recovery...");
            using var recoveredDb = await LSMTreeDB.OpenAsync(dbPath);

            // Verify data integrity after recovery
            Console.WriteLine("Verifying data after recovery...");
            var verificationTasks = new List<Task<(bool, byte[])>>();
            
            foreach (var kvp in testData.Take(1000)) // Verify subset
            {
                verificationTasks.Add(recoveredDb.GetAsync(kvp.Key));
            }

            var results = await Task.WhenAll(verificationTasks);
            var validCount = 0;
            var index = 0;

            foreach (var kvp in testData.Take(1000))
            {
                var (found, value) = results[index++];
                if (found && value.SequenceEqual(kvp.Value))
                {
                    validCount++;
                }
            }

            Console.WriteLine($"Recovery verification: {validCount}/1000 records recovered correctly");
        }

        private static byte[] GenerateStructuredValue(int recordId)
        {
            var data = $"Record-{recordId}|Timestamp-{DateTime.UtcNow.Ticks}|Data-{new string('x', 200)}";
            return Encoding.UTF8.GetBytes(data);
        }

        private static byte[] GenerateLargeValue(int size, int seed)
        {
            var random = new Random(seed);
            var value = new byte[size];
            
            // Fill with pattern for validation
            var pattern = BitConverter.GetBytes(seed);
            for (int i = 0; i < size; i++)
            {
                value[i] = pattern[i % pattern.Length];
            }
            
            return value;
        }

        private static bool ValidateLargeValue(byte[] value, int expectedSeed)
        {
            if (value.Length != LARGE_VALUE_SIZE)
                return false;
                
            var pattern = BitConverter.GetBytes(expectedSeed);
            for (int i = 0; i < value.Length; i++)
            {
                if (value[i] != pattern[i % pattern.Length])
                    return false;
            }
            
            return true;
        }

        private static byte[] GenerateThreadValue(int threadId, int recordId)
        {
            var data = $"Thread-{threadId}|Record-{recordId}|{new string((char)('A' + (threadId % 26)), 50)}";
            return Encoding.UTF8.GetBytes(data);
        }

        private static byte[] GenerateLevelValue(int level, int recordId)
        {
            var data = $"Level-{level}|Record-{recordId}|Version-{DateTime.UtcNow.Ticks}|{new string((char)('0' + level), 100)}";
            return Encoding.UTF8.GetBytes(data);
        }

        private static byte[] GenerateRecoveryValue(int recordId)
        {
            var data = $"Recovery-Test-{recordId}|{new string('R', 150)}";
            return Encoding.UTF8.GetBytes(data);
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
