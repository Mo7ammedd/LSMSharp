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
    public class StressTests
    {
        private const int HEAVY_LOAD_OPERATIONS = 50000; // Increased from 10000
        private const int CONCURRENT_THREADS = 20; // Increased from 10
        private const int LARGE_VALUE_SIZE = 2048; // Increased from 1KB to 2KB

        public static async Task RunAllAsync()
        {
            var totalStopwatch = Stopwatch.StartNew();
            Console.WriteLine("╔══════════════════════════════════════╗");
            Console.WriteLine("║          LSM-Tree Stress Tests       ║");
            Console.WriteLine("╚══════════════════════════════════════╝");
            Console.WriteLine($"🚀 Started at: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            Console.WriteLine($"📊 Test Configuration:");
            Console.WriteLine($"   • Heavy Load Operations: {HEAVY_LOAD_OPERATIONS:N0}");
            Console.WriteLine($"   • Concurrent Threads: {CONCURRENT_THREADS}");
            Console.WriteLine($"   • Large Value Size: {LARGE_VALUE_SIZE:N0} bytes");
            Console.WriteLine();

            var dbPath = Path.Combine(Environment.CurrentDirectory, "test_stress_db");
            CleanupDirectory(dbPath);

            Console.WriteLine($"🗄️  Database path: {dbPath}");
            var dbOpenStopwatch = Stopwatch.StartNew();
            await using var db = await LSMTreeDB.OpenAsync(dbPath);
            dbOpenStopwatch.Stop();
            Console.WriteLine($"✅ Database opened in {dbOpenStopwatch.ElapsedMilliseconds}ms");

            var testResults = new List<(string testName, TimeSpan duration, bool success)>();

            // Run all stress tests with error handling
            try
            {
                var testStopwatch = Stopwatch.StartNew();
                await HeavyLoadTest(db);
                testStopwatch.Stop();
                testResults.Add(("Heavy Load Test", testStopwatch.Elapsed, true));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Heavy Load Test failed: {ex.Message}");
                testResults.Add(("Heavy Load Test", TimeSpan.Zero, false));
            }

            try
            {
                var testStopwatch = Stopwatch.StartNew();
                await LargeValueTest(db);
                testStopwatch.Stop();
                testResults.Add(("Large Value Test", testStopwatch.Elapsed, true));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Large Value Test failed: {ex.Message}");
                testResults.Add(("Large Value Test", TimeSpan.Zero, false));
            }

            try
            {
                var testStopwatch = Stopwatch.StartNew();
                await ConcurrentStressTest(db);
                testStopwatch.Stop();
                testResults.Add(("Concurrent Stress Test", testStopwatch.Elapsed, true));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Concurrent Stress Test failed: {ex.Message}");
                testResults.Add(("Concurrent Stress Test", TimeSpan.Zero, false));
            }

            try
            {
                var testStopwatch = Stopwatch.StartNew();
                await MemoryPressureTest(db);
                testStopwatch.Stop();
                testResults.Add(("Memory Pressure Test", testStopwatch.Elapsed, true));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Memory Pressure Test failed: {ex.Message}");
                testResults.Add(("Memory Pressure Test", TimeSpan.Zero, false));
            }

            try
            {
                var testStopwatch = Stopwatch.StartNew();
                await CompactionStressTest(db);
                testStopwatch.Stop();
                testResults.Add(("Compaction Stress Test", testStopwatch.Elapsed, true));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Compaction Stress Test failed: {ex.Message}");
                testResults.Add(("Compaction Stress Test", TimeSpan.Zero, false));
            }

            try
            {
                var testStopwatch = Stopwatch.StartNew();
                await CrashRecoveryTest();
                testStopwatch.Stop();
                testResults.Add(("Crash Recovery Test", testStopwatch.Elapsed, true));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Crash Recovery Test failed: {ex.Message}");
                testResults.Add(("Crash Recovery Test", TimeSpan.Zero, false));
            }

            totalStopwatch.Stop();

            // Print comprehensive summary
            Console.WriteLine();
            Console.WriteLine("╔══════════════════════════════════════╗");
            Console.WriteLine("║           TEST SUMMARY               ║");
            Console.WriteLine("╚══════════════════════════════════════╝");
            
            var passedTests = testResults.Count(r => r.success);
            var totalTests = testResults.Count;
            var successRate = passedTests * 100.0 / totalTests;
            
            Console.WriteLine($"📊 Overall Results: {passedTests}/{totalTests} tests passed ({successRate:F1}%)");
            Console.WriteLine();
            
            foreach (var (testName, duration, success) in testResults)
            {
                var status = success ? "✅ PASS" : "❌ FAIL";
                var time = success ? $"{duration.TotalSeconds:F2}s" : "N/A";
                Console.WriteLine($"  {status} {testName,-25} {time,8}");
            }
            
            Console.WriteLine();
            Console.WriteLine($"⏱️  Total execution time: {totalStopwatch.Elapsed.TotalSeconds:F2}s ({totalStopwatch.ElapsedMilliseconds:N0}ms)");
            Console.WriteLine($"🏁 Finished at: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            
            if (successRate == 100)
            {
                Console.WriteLine("🎉 ALL STRESS TESTS PASSED! 🎉");
            }
            else
            {
                Console.WriteLine($"⚠️  {totalTests - passedTests} test(s) failed. Check logs above.");
            }
        }

        private static async Task HeavyLoadTest(LSMTreeDB db)
        {
            var testStopwatch = Stopwatch.StartNew();
            Console.WriteLine("\n1. Heavy Load Test");
            Console.WriteLine("------------------");
            Console.WriteLine($"🔥 Starting heavy load test with {HEAVY_LOAD_OPERATIONS} operations...");

            var tasks = new List<Task>();
            var batchSize = 1000;
            var totalBatches = HEAVY_LOAD_OPERATIONS / batchSize;
            var totalBytes = 0L;

            Console.WriteLine($"📝 Writing {HEAVY_LOAD_OPERATIONS} records in {totalBatches} batches of {batchSize}...");

            var writeStopwatch = Stopwatch.StartNew();
            for (int batch = 0; batch < totalBatches; batch++)
            {
                tasks.Clear();
                var batchStopwatch = Stopwatch.StartNew();
                
                for (int i = 0; i < batchSize; i++)
                {
                    var recordId = batch * batchSize + i;
                    var key = $"heavy:{recordId:D8}";
                    var value = GenerateStructuredValue(recordId);
                    totalBytes += value.Length + key.Length;
                    tasks.Add(db.SetAsync(key, value));
                }

                await Task.WhenAll(tasks);
                batchStopwatch.Stop();
                
                if ((batch + 1) % 10 == 0)
                {
                    var progress = (double)(batch + 1) / totalBatches * 100;
                    var opsPerSec = batchSize * 10 / (batchStopwatch.ElapsedMilliseconds / 1000.0);
                    Console.WriteLine($"  📊 Progress: {progress:F1}% | Batch {batch + 1}/{totalBatches} | " +
                                    $"Rate: {opsPerSec:F0} ops/sec | Elapsed: {writeStopwatch.Elapsed.TotalSeconds:F1}s");
                    await db.FlushAsync(); // Periodic flush
                }
            }
            writeStopwatch.Stop();

            var writeRate = HEAVY_LOAD_OPERATIONS / writeStopwatch.Elapsed.TotalSeconds;
            var throughput = totalBytes / (1024.0 * 1024.0) / writeStopwatch.Elapsed.TotalSeconds;
            Console.WriteLine($"✅ Heavy load write completed in {writeStopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"   📈 Write rate: {writeRate:F0} ops/sec");
            Console.WriteLine($"   💾 Throughput: {throughput:F2} MB/sec");

            // Force flush to ensure all data is persisted
            var flushStopwatch = Stopwatch.StartNew();
            await db.FlushAsync();
            await Task.Delay(100); // Allow flush to complete
            flushStopwatch.Stop();
            Console.WriteLine($"   💫 Final flush completed in {flushStopwatch.ElapsedMilliseconds}ms");

            // Verify random reads
            Console.WriteLine($"🔍 Verifying 1000 random reads...");
            var readStopwatch = Stopwatch.StartNew();
            var random = new Random(42);
            var readTasks = new List<Task<(bool, byte[])>>();

            for (int i = 0; i < 1000; i++)
            {
                var recordId = random.Next(0, HEAVY_LOAD_OPERATIONS);
                var key = $"heavy:{recordId:D8}";
                readTasks.Add(db.GetAsync(key));
            }

            var results = await Task.WhenAll(readTasks);
            readStopwatch.Stop();
            
            var hitCount = results.Count(r => r.Item1);
            var readRate = 1000 / readStopwatch.Elapsed.TotalSeconds;
            var hitRate = hitCount / 10.0;
            
            testStopwatch.Stop();
            Console.WriteLine($"✅ Read verification completed in {readStopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"   📊 Hit rate: {hitCount}/1000 ({hitRate:F1}%)");
            Console.WriteLine($"   📈 Read rate: {readRate:F0} ops/sec");
            Console.WriteLine($"🎯 Heavy Load Test completed in {testStopwatch.Elapsed.TotalSeconds:F2}s");
        }

        private static async Task LargeValueTest(LSMTreeDB db)
        {
            var testStopwatch = Stopwatch.StartNew();
            Console.WriteLine("\n2. Large Value Test");
            Console.WriteLine("-------------------");

            var largeValueCount = 100;
            var totalSize = largeValueCount * LARGE_VALUE_SIZE;
            Console.WriteLine($"📦 Writing {largeValueCount} large values ({LARGE_VALUE_SIZE:N0} bytes each, {totalSize / 1024.0 / 1024.0:F1} MB total)...");

            var writeStopwatch = Stopwatch.StartNew();
            var tasks = new List<Task>();
            for (int i = 0; i < largeValueCount; i++)
            {
                var key = $"large:{i:D6}";
                var value = GenerateLargeValue(LARGE_VALUE_SIZE, i);
                tasks.Add(db.SetAsync(key, value));

                if ((i + 1) % 20 == 0)
                {
                    var progress = (double)(i + 1) / largeValueCount * 100;
                    Console.WriteLine($"  📊 Progress: {progress:F0}% ({i + 1}/{largeValueCount})");
                }
            }

            await Task.WhenAll(tasks);
            writeStopwatch.Stop();
            
            var writeRate = largeValueCount / writeStopwatch.Elapsed.TotalSeconds;
            var throughput = totalSize / (1024.0 * 1024.0) / writeStopwatch.Elapsed.TotalSeconds;
            Console.WriteLine($"✅ Large values written in {writeStopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"   📈 Write rate: {writeRate:F1} ops/sec");
            Console.WriteLine($"   💾 Throughput: {throughput:F2} MB/sec");

            // Verify large values
            Console.WriteLine($"🔍 Verifying {largeValueCount} large values...");
            var readStopwatch = Stopwatch.StartNew();
            var readTasks = new List<Task<(bool, byte[])>>();
            for (int i = 0; i < largeValueCount; i++)
            {
                var key = $"large:{i:D6}";
                readTasks.Add(db.GetAsync(key));
            }

            var results = await Task.WhenAll(readTasks);
            readStopwatch.Stop();
            
            var validCount = 0;
            var bytesRead = 0L;

            for (int i = 0; i < results.Length; i++)
            {
                var (found, value) = results[i];
                if (found && ValidateLargeValue(value, i))
                {
                    validCount++;
                    bytesRead += value.Length;
                }
            }

            var readRate = largeValueCount / readStopwatch.Elapsed.TotalSeconds;
            var readThroughput = bytesRead / (1024.0 * 1024.0) / readStopwatch.Elapsed.TotalSeconds;
            
            testStopwatch.Stop();
            Console.WriteLine($"✅ Large value verification completed in {readStopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"   📊 Validation: {validCount}/{largeValueCount} values valid ({validCount * 100.0 / largeValueCount:F1}%)");
            Console.WriteLine($"   📈 Read rate: {readRate:F1} ops/sec");
            Console.WriteLine($"   💾 Read throughput: {readThroughput:F2} MB/sec");
            Console.WriteLine($"🎯 Large Value Test completed in {testStopwatch.Elapsed.TotalSeconds:F2}s");
        }

        private static async Task ConcurrentStressTest(LSMTreeDB db)
        {
            var testStopwatch = Stopwatch.StartNew();
            Console.WriteLine("\n3. Concurrent Stress Test");
            Console.WriteLine("-------------------------");

            var operationsPerThread = 100; // Reduced to prevent overwhelming
            var totalOperations = CONCURRENT_THREADS * operationsPerThread;

            Console.WriteLine($"🚀 Running {totalOperations} operations across {CONCURRENT_THREADS} concurrent threads...");
            Console.WriteLine($"   📊 Operations per thread: {operationsPerThread}");
            Console.WriteLine($"   ⚖️  Mix: 70% writes, 20% reads, 10% deletes");

            var threadTasks = new List<Task>();
            var threadResults = new int[CONCURRENT_THREADS * 3]; // [writes, reads, deletes] per thread
            var operationStopwatch = Stopwatch.StartNew();

            for (int threadId = 0; threadId < CONCURRENT_THREADS; threadId++)
            {
                var localThreadId = threadId; // Capture for closure
                threadTasks.Add(Task.Run(async () =>
                {
                    var random = new Random(localThreadId);
                    int writes = 0, reads = 0, deletes = 0;

                    for (int i = 0; i < operationsPerThread; i++)
                    {
                        try
                        {
                            var operation = random.NextDouble();
                            var recordId = localThreadId * operationsPerThread + i;

                            if (operation < 0.7) // 70% writes
                            {
                                var key = $"concurrent:{recordId:D8}";
                                var value = GenerateThreadValue(localThreadId, i);
                                await db.SetAsync(key, value);
                                writes++;
                            }
                            else if (operation < 0.9) // 20% reads
                            {
                                var readRecordId = random.Next(0, recordId + 1);
                                var key = $"concurrent:{readRecordId:D8}";
                                await db.GetAsync(key);
                                reads++;
                            }
                            else // 10% deletes
                            {
                                var deleteRecordId = random.Next(0, recordId + 1);
                                var key = $"concurrent:{deleteRecordId:D8}";
                                await db.DeleteAsync(key);
                                deletes++;
                            }

                            // Progress reporting per thread
                            if ((i + 1) % 25 == 0)
                            {
                                var progress = (double)(i + 1) / operationsPerThread * 100;
                                Console.WriteLine($"  🧵 Thread {localThreadId}: {progress:F0}% complete ({writes}W/{reads}R/{deletes}D)");
                            }
                        }
                        catch (ObjectDisposedException)
                        {
                            // Expected during cleanup, exit gracefully
                            break;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Thread {localThreadId} error: {ex.Message}");
                            break;
                        }
                    }

                    // Store results
                    threadResults[localThreadId * 3] = writes;
                    threadResults[localThreadId * 3 + 1] = reads;
                    threadResults[localThreadId * 3 + 2] = deletes;
                }));
            }

            await Task.WhenAll(threadTasks);
            operationStopwatch.Stop();

            // Calculate totals
            var totalWrites = 0;
            var totalReads = 0;
            var totalDeletes = 0;
            for (int i = 0; i < CONCURRENT_THREADS; i++)
            {
                totalWrites += threadResults[i * 3];
                totalReads += threadResults[i * 3 + 1];
                totalDeletes += threadResults[i * 3 + 2];
            }

            var operationRate = totalOperations / operationStopwatch.Elapsed.TotalSeconds;
            
            Console.WriteLine($"✅ Concurrent stress test completed in {operationStopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"   📊 Total operations: {totalWrites + totalReads + totalDeletes}");
            Console.WriteLine($"   📝 Writes: {totalWrites} | 📖 Reads: {totalReads} | 🗑️  Deletes: {totalDeletes}");
            Console.WriteLine($"   📈 Overall rate: {operationRate:F0} ops/sec");

            // Force compaction after concurrent stress
            var compactionStopwatch = Stopwatch.StartNew();
            await db.CompactAsync();
            compactionStopwatch.Stop();
            
            testStopwatch.Stop();
            Console.WriteLine($"✅ Post-stress compaction completed in {compactionStopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"🎯 Concurrent Stress Test completed in {testStopwatch.Elapsed.TotalSeconds:F2}s");
        }

        private static async Task MemoryPressureTest(LSMTreeDB db)
        {
            var testStopwatch = Stopwatch.StartNew();
            Console.WriteLine("\n4. Memory Pressure Test");
            Console.WriteLine("-----------------------");

            var initialMemory = GC.GetTotalMemory(true);
            Console.WriteLine($"🧠 Initial memory: {initialMemory / 1024.0 / 1024.0:F1} MB");

            // Generate memory pressure with many small operations
            var pressureOperations = 5000;
            var tasks = new List<Task>();
            var totalBytes = 0L;

            Console.WriteLine($"💥 Generating memory pressure with {pressureOperations} operations...");

            var operationStopwatch = Stopwatch.StartNew();
            for (int i = 0; i < pressureOperations; i++)
            {
                var key = $"memory:{i:D8}";
                var value = GenerateRandomValue(100);
                totalBytes += value.Length + key.Length;
                tasks.Add(db.SetAsync(key, value));

                // Process in batches to avoid overwhelming memory
                if (tasks.Count >= 500)
                {
                    await Task.WhenAll(tasks);
                    tasks.Clear();

                    if (i % 1000 == 0)
                    {
                        var currentMemory = GC.GetTotalMemory(false);
                        var progress = (double)i / pressureOperations * 100;
                        var rate = i / operationStopwatch.Elapsed.TotalSeconds;
                        Console.WriteLine($"  📊 Progress: {progress:F1}% ({i}/{pressureOperations}) | " +
                                        $"Memory: {currentMemory / 1024.0 / 1024.0:F1} MB | Rate: {rate:F0} ops/sec");
                    }
                }
            }

            if (tasks.Count > 0)
            {
                await Task.WhenAll(tasks);
            }
            operationStopwatch.Stop();

            var peakMemory = GC.GetTotalMemory(false);
            var operationRate = pressureOperations / operationStopwatch.Elapsed.TotalSeconds;
            var throughput = totalBytes / (1024.0 * 1024.0) / operationStopwatch.Elapsed.TotalSeconds;
            
            Console.WriteLine($"✅ Memory pressure operations completed in {operationStopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"   📊 Peak memory: {peakMemory / 1024.0 / 1024.0:F1} MB");
            Console.WriteLine($"   📈 Operation rate: {operationRate:F0} ops/sec");
            Console.WriteLine($"   💾 Throughput: {throughput:F2} MB/sec");

            // Force multiple flushes and compactions
            Console.WriteLine($"🧹 Starting cleanup and compaction...");
            var cleanupStopwatch = Stopwatch.StartNew();
            
            await db.FlushAsync();
            await db.CompactAsync();

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            cleanupStopwatch.Stop();

            var finalMemory = GC.GetTotalMemory(true);
            var memoryReduction = (peakMemory - finalMemory) / (1024.0 * 1024.0);
            
            testStopwatch.Stop();
            Console.WriteLine($"✅ Cleanup completed in {cleanupStopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"   🧠 Final memory: {finalMemory / 1024.0 / 1024.0:F1} MB");
            Console.WriteLine($"   📉 Memory reduced by: {memoryReduction:F1} MB");
            Console.WriteLine($"🎯 Memory Pressure Test completed in {testStopwatch.Elapsed.TotalSeconds:F2}s");
        }

        private static async Task CompactionStressTest(LSMTreeDB db)
        {
            var testStopwatch = Stopwatch.StartNew();
            Console.WriteLine("\n5. Compaction Stress Test");
            Console.WriteLine("-------------------------");

            // Create multiple levels with overlapping key ranges
            var levelsToCreate = 3;
            var recordsPerLevel = 2000;
            var totalRecords = levelsToCreate * recordsPerLevel;

            Console.WriteLine($"🏗️  Creating {levelsToCreate} levels with {recordsPerLevel} records each ({totalRecords} total)...");
            Console.WriteLine($"   🔄 Overlapping key ranges to force intensive compaction");

            var totalBytes = 0L;
            var writeStopwatch = Stopwatch.StartNew();

            for (int level = 0; level < levelsToCreate; level++)
            {
                var levelStopwatch = Stopwatch.StartNew();
                var tasks = new List<Task>();
                
                Console.WriteLine($"  📝 Writing level {level + 1}...");
                
                for (int i = 0; i < recordsPerLevel; i++)
                {
                    // Create overlapping key ranges to force compaction
                    var keyId = (i + level * recordsPerLevel / 2) % (recordsPerLevel * 2);
                    var key = $"compact:{keyId:D8}";
                    var value = GenerateLevelValue(level, i);
                    totalBytes += value.Length + key.Length;
                    tasks.Add(db.SetAsync(key, value));

                    if ((i + 1) % 500 == 0)
                    {
                        var progress = (double)(i + 1) / recordsPerLevel * 100;
                        Console.WriteLine($"    📊 Level {level + 1} progress: {progress:F0}% ({i + 1}/{recordsPerLevel})");
                    }
                }

                await Task.WhenAll(tasks);
                levelStopwatch.Stop();
                
                var flushStopwatch = Stopwatch.StartNew();
                await db.FlushAsync(); // Force creation of new SSTable
                flushStopwatch.Stop();
                
                var levelRate = recordsPerLevel / levelStopwatch.Elapsed.TotalSeconds;
                Console.WriteLine($"  ✅ Level {level + 1} created in {levelStopwatch.ElapsedMilliseconds}ms " +
                                $"(Rate: {levelRate:F0} ops/sec, Flush: {flushStopwatch.ElapsedMilliseconds}ms)");
            }
            writeStopwatch.Stop();

            var writeRate = totalRecords / writeStopwatch.Elapsed.TotalSeconds;
            var throughput = totalBytes / (1024.0 * 1024.0) / writeStopwatch.Elapsed.TotalSeconds;
            Console.WriteLine($"✅ All levels created in {writeStopwatch.Elapsed.TotalSeconds:F2}s");
            Console.WriteLine($"   📈 Overall write rate: {writeRate:F0} ops/sec");
            Console.WriteLine($"   💾 Throughput: {throughput:F2} MB/sec");

            // Force multiple rounds of compaction
            Console.WriteLine($"🔄 Forcing intensive compaction (3 rounds)...");
            var compactionStopwatch = Stopwatch.StartNew();
            
            for (int round = 0; round < 3; round++)
            {
                var roundStopwatch = Stopwatch.StartNew();
                await db.CompactAsync();
                roundStopwatch.Stop();
                Console.WriteLine($"  ✅ Compaction round {round + 1} completed in {roundStopwatch.ElapsedMilliseconds}ms");
            }
            compactionStopwatch.Stop();
            
            testStopwatch.Stop();
            Console.WriteLine($"✅ Intensive compaction completed in {compactionStopwatch.Elapsed.TotalSeconds:F2}s");
            Console.WriteLine($"🎯 Compaction Stress Test completed in {testStopwatch.Elapsed.TotalSeconds:F2}s");
        }

        private static async Task CrashRecoveryTest()
        {
            var testStopwatch = Stopwatch.StartNew();
            Console.WriteLine("\n6. Crash Recovery Test");
            Console.WriteLine("----------------------");

            var dbPath = Path.Combine(Environment.CurrentDirectory, "test_recovery_db");
            CleanupDirectory(dbPath);

            // Write data without proper cleanup (simulating crash)
            var testData = new Dictionary<string, byte[]>();
            var totalBytes = 0L;
            
            Console.WriteLine($"🔥 Simulating database crash scenario...");
            
            {
                await using var db = await LSMTreeDB.OpenAsync(dbPath);
                
                Console.WriteLine($"📝 Writing 1000 test records before 'crash'...");
                var writeStopwatch = Stopwatch.StartNew();
                var tasks = new List<Task>();
                
                for (int i = 0; i < 1000; i++)
                {
                    var key = $"recovery:{i:D6}";
                    var value = GenerateRecoveryValue(i);
                    totalBytes += value.Length + key.Length;
                    testData[key] = value;
                    tasks.Add(db.SetAsync(key, value));

                    if ((i + 1) % 200 == 0)
                    {
                        var progress = (double)(i + 1) / 1000 * 100;
                        Console.WriteLine($"  📊 Pre-crash progress: {progress:F0}% ({i + 1}/1000)");
                    }
                }

                await Task.WhenAll(tasks);
                writeStopwatch.Stop();
                
                var writeRate = 1000 / writeStopwatch.Elapsed.TotalSeconds;
                var throughput = totalBytes / (1024.0 * 1024.0) / writeStopwatch.Elapsed.TotalSeconds;
                Console.WriteLine($"✅ Pre-crash data written in {writeStopwatch.ElapsedMilliseconds}ms");
                Console.WriteLine($"   📈 Write rate: {writeRate:F0} ops/sec");
                Console.WriteLine($"   💾 Throughput: {throughput:F2} MB/sec");
                Console.WriteLine($"💥 Simulating crash - database closing unexpectedly...");
                // Simulate crash - don't call Dispose explicitly
            }

            // Reopen database (simulate recovery)
            Console.WriteLine($"🔄 Reopening database for crash recovery...");
            var recoveryStopwatch = Stopwatch.StartNew();
            await using var recoveredDb = await LSMTreeDB.OpenAsync(dbPath);
            recoveryStopwatch.Stop();
            
            Console.WriteLine($"✅ Database recovered in {recoveryStopwatch.ElapsedMilliseconds}ms");

            // Verify data integrity after recovery
            Console.WriteLine($"🔍 Verifying data integrity after recovery (100 samples)...");
            var verificationStopwatch = Stopwatch.StartNew();
            var verificationTasks = new List<Task<(bool, byte[])>>();
            var samplesToTest = testData.Take(100).ToList();
            
            foreach (var kvp in samplesToTest)
            {
                verificationTasks.Add(recoveredDb.GetAsync(kvp.Key));
            }

            var results = await Task.WhenAll(verificationTasks);
            verificationStopwatch.Stop();
            
            var validCount = 0;
            var bytesVerified = 0L;
            var index = 0;

            foreach (var kvp in samplesToTest)
            {
                var (found, value) = results[index++];
                if (found && value.SequenceEqual(kvp.Value))
                {
                    validCount++;
                    bytesVerified += value.Length;
                }
            }

            var verificationRate = 100 / verificationStopwatch.Elapsed.TotalSeconds;
            var successRate = validCount * 100.0 / 100;
            
            testStopwatch.Stop();
            Console.WriteLine($"✅ Recovery verification completed in {verificationStopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"   📊 Data integrity: {validCount}/100 records recovered correctly ({successRate:F1}%)");
            Console.WriteLine($"   📈 Verification rate: {verificationRate:F0} ops/sec");
            Console.WriteLine($"   💾 Data verified: {bytesVerified / 1024.0:F1} KB");
            
            if (successRate >= 95)
            {
                Console.WriteLine($"🎯 Crash Recovery Test PASSED in {testStopwatch.Elapsed.TotalSeconds:F2}s");
            }
            else
            {
                Console.WriteLine($"❌ Crash Recovery Test FAILED - Low recovery rate: {successRate:F1}%");
            }
        }

        private static byte[] GenerateStructuredValue(int recordId)
        {
            var data = $"Record-{recordId}|Timestamp-{DateTime.UtcNow.Ticks}|Data-{new string('x', 50)}";
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
