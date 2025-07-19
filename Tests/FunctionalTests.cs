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
    public class FunctionalTests
    {
        public static async Task RunAllAsync()
        {
            var totalStopwatch = Stopwatch.StartNew();
            Console.WriteLine("LSM-Tree Functional Tests");
            Console.WriteLine("=========================");
            Console.WriteLine($"Started at: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            Console.WriteLine();

            var dbPath = Path.Combine(Environment.CurrentDirectory, "test_functional_db");
            CleanupDirectory(dbPath);

            await using var db = await LSMTreeDB.OpenAsync(dbPath);

            await BasicOperationsTest(db);
            await UpdateOperationsTest(db);
            await DeleteOperationsTest(db);
            await RangeQueryTest(db);
            await TransactionConsistencyTest(db);
            await DataIntegrityTest(db);
            await SimpleIntegrityTest();
            await EdgeCaseTests(db);

            totalStopwatch.Stop();
            Console.WriteLine();
            Console.WriteLine($"Functional tests completed in {totalStopwatch.ElapsedMilliseconds}ms ({totalStopwatch.Elapsed.TotalSeconds:F2}s)!");
            Console.WriteLine($"Finished at: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        }

        private static async Task BasicOperationsTest(LSMTreeDB db)
        {
            var testStopwatch = Stopwatch.StartNew();
            Console.WriteLine("1. Basic Operations Test");
            Console.WriteLine("-----------------------");

            // Test basic Put/Get operations with increased test cases
            var testCases = new[]
            {
                ("key1", "value1"),
                ("key2", "value2"),
                ("key3", "value3"),
                ("space_key", "empty_key_value"),
                ("very_long_key_" + new string('x', 1000), "long_key_value"),
                ("key_with_special_chars_!@#$%^&*()", "special_value"),
                ("numeric_key_123", "numeric_value_456"),
                ("mixed_case_Key", "Mixed_Case_Value"),
                ("key.with.dots", "dotted.value"),
                ("key-with-dashes", "dashed-value"),
                ("key_with_underscores", "underscore_value"),
                ("key/with/slashes", "slash/value")
            };

            Console.WriteLine($"Testing {testCases.Length} basic operations...");

            var insertStopwatch = Stopwatch.StartNew();
            // Insert all test cases
            var insertTasks = new List<Task>();
            foreach (var (key, value) in testCases)
            {
                insertTasks.Add(db.SetAsync(key, Encoding.UTF8.GetBytes(value)));
            }
            await Task.WhenAll(insertTasks);
            insertStopwatch.Stop();
            Console.WriteLine($"✓ Inserted {testCases.Length} records in {insertStopwatch.ElapsedMilliseconds}ms");

            var verifyStopwatch = Stopwatch.StartNew();
            // Verify all test cases
            var verifyTasks = new List<Task<(bool, byte[])>>();
            foreach (var (key, _) in testCases)
            {
                verifyTasks.Add(db.GetAsync(key));
            }
            var results = await Task.WhenAll(verifyTasks);
            verifyStopwatch.Stop();
            Console.WriteLine($"✓ Verified {testCases.Length} records in {verifyStopwatch.ElapsedMilliseconds}ms");

            int passCount = 0;
            for (int i = 0; i < testCases.Length; i++)
            {
                var (found, value) = results[i];
                var expectedValue = testCases[i].Item2;
                
                if (found && Encoding.UTF8.GetString(value) == expectedValue)
                {
                    passCount++;
                }
                else
                {
                    Console.WriteLine($"❌ FAIL: Key '{testCases[i].Item1}' - Expected: '{expectedValue}', Got: '{(found ? Encoding.UTF8.GetString(value) : "NOT_FOUND")}'");
                }
            }

            testStopwatch.Stop();
            Console.WriteLine($"✅ Basic operations: {passCount}/{testCases.Length} tests passed");
            Console.WriteLine($"   Total time: {testStopwatch.ElapsedMilliseconds}ms, Insert rate: {testCases.Length * 1000.0 / insertStopwatch.ElapsedMilliseconds:F0} ops/sec, Read rate: {testCases.Length * 1000.0 / verifyStopwatch.ElapsedMilliseconds:F0} ops/sec");
            Console.WriteLine();
        }

        private static async Task UpdateOperationsTest(LSMTreeDB db)
        {
            var testStopwatch = Stopwatch.StartNew();
            Console.WriteLine("2. Update Operations Test");
            Console.WriteLine("------------------------");

            var key = "update_test_key";
            var versions = new[] { "version1", "version2", "version3", "version4", "version5", "version6", "final_version" };

            Console.WriteLine($"Testing {versions.Length} sequential updates on same key...");

            // Update the same key multiple times
            var updateTimes = new List<long>();
            for (int i = 0; i < versions.Length; i++)
            {
                var updateStopwatch = Stopwatch.StartNew();
                await db.SetAsync(key, Encoding.UTF8.GetBytes(versions[i]));
                updateStopwatch.Stop();
                updateTimes.Add(updateStopwatch.ElapsedMilliseconds);
                
                var verifyStopwatch = Stopwatch.StartNew();
                var (found, value) = await db.GetAsync(key);
                verifyStopwatch.Stop();
                var actualValue = found ? Encoding.UTF8.GetString(value) : "NOT_FOUND";
                
                if (actualValue == versions[i])
                {
                    Console.WriteLine($"✓ Update {i + 1}/{versions.Length}: PASS - Value is '{actualValue}' (update: {updateStopwatch.ElapsedMilliseconds}ms, verify: {verifyStopwatch.ElapsedMilliseconds}ms)");
                }
                else
                {
                    Console.WriteLine($"❌ Update {i + 1}/{versions.Length}: FAIL - Expected '{versions[i]}', Got '{actualValue}'");
                }
            }

            // Force flush and verify persistence
            var flushStopwatch = Stopwatch.StartNew();
            await db.FlushAsync();
            flushStopwatch.Stop();
            
            var (finalFound, finalValue) = await db.GetAsync(key);
            var finalActual = finalFound ? Encoding.UTF8.GetString(finalValue) : "NOT_FOUND";
            
            testStopwatch.Stop();
            Console.WriteLine($"✓ Flush completed in {flushStopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"✅ After flush: {(finalActual == versions.Last() ? "PASS" : "FAIL")} - Value is '{finalActual}'");
            Console.WriteLine($"   Total time: {testStopwatch.ElapsedMilliseconds}ms, Average update time: {updateTimes.Average():F1}ms");
            Console.WriteLine();
        }

        private static async Task DeleteOperationsTest(LSMTreeDB db)
        {
            var testStopwatch = Stopwatch.StartNew();
            Console.WriteLine("3. Delete Operations Test");
            Console.WriteLine("------------------------");

            var testKeys = new[] { "delete1", "delete2", "delete3", "delete4", "delete5", "delete6", "delete7", "delete8" };
            var testValues = new[] { "value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8" };

            Console.WriteLine($"Testing delete operations on {testKeys.Length} records...");

            // Insert test data
            var insertStopwatch = Stopwatch.StartNew();
            var insertTasks = new List<Task>();
            for (int i = 0; i < testKeys.Length; i++)
            {
                insertTasks.Add(db.SetAsync(testKeys[i], Encoding.UTF8.GetBytes(testValues[i])));
            }
            await Task.WhenAll(insertTasks);
            insertStopwatch.Stop();
            Console.WriteLine($"✓ Inserted {testKeys.Length} records in {insertStopwatch.ElapsedMilliseconds}ms");

            // Verify insertion
            var verifyCount = 0;
            for (int i = 0; i < testKeys.Length; i++)
            {
                var (found, value) = await db.GetAsync(testKeys[i]);
                if (found)
                {
                    verifyCount++;
                    Console.WriteLine($"  Before delete - {testKeys[i]}: FOUND");
                }
                else
                {
                    Console.WriteLine($"  Before delete - {testKeys[i]}: NOT_FOUND (❌)");
                }
            }

            // Delete keys one by one and measure timing
            var deleteTimes = new List<long>();
            var deleteCount = 0;
            for (int i = 0; i < testKeys.Length; i++)
            {
                var deleteStopwatch = Stopwatch.StartNew();
                await db.DeleteAsync(testKeys[i]);
                deleteStopwatch.Stop();
                deleteTimes.Add(deleteStopwatch.ElapsedMilliseconds);
                
                var (found, _) = await db.GetAsync(testKeys[i]);
                if (!found)
                {
                    deleteCount++;
                    Console.WriteLine($"✓ After deleting {testKeys[i]}: NOT_FOUND (PASS) - {deleteStopwatch.ElapsedMilliseconds}ms");
                }
                else
                {
                    Console.WriteLine($"❌ After deleting {testKeys[i]}: STILL_FOUND (FAIL)");
                }
            }

            // Test deleting non-existent key
            var nonExistentStopwatch = Stopwatch.StartNew();
            await db.DeleteAsync("non_existent_key");
            nonExistentStopwatch.Stop();
            Console.WriteLine($"✓ Delete non-existent key: PASS (no exception) - {nonExistentStopwatch.ElapsedMilliseconds}ms");

            // Force flush and verify deletes persist
            var flushStopwatch = Stopwatch.StartNew();
            await db.FlushAsync();
            flushStopwatch.Stop();
            
            var persistCount = 0;
            for (int i = 0; i < testKeys.Length; i++)
            {
                var (found, _) = await db.GetAsync(testKeys[i]);
                if (!found)
                {
                    persistCount++;
                    Console.WriteLine($"  After flush - {testKeys[i]}: NOT_FOUND (PASS)");
                }
                else
                {
                    Console.WriteLine($"  After flush - {testKeys[i]}: STILL_FOUND (❌ FAIL)");
                }
            }

            testStopwatch.Stop();
            Console.WriteLine($"✅ Delete operations: {deleteCount}/{testKeys.Length} deletions successful, {persistCount}/{testKeys.Length} persist after flush");
            Console.WriteLine($"   Total time: {testStopwatch.ElapsedMilliseconds}ms, Flush time: {flushStopwatch.ElapsedMilliseconds}ms, Average delete time: {deleteTimes.Average():F1}ms");
            Console.WriteLine();
        }

        private static async Task RangeQueryTest(LSMTreeDB db)
        {
            var testStopwatch = Stopwatch.StartNew();
            Console.WriteLine("4. Range Query Test");
            Console.WriteLine("------------------");

            // Insert sorted test data - increased from 20 to 100 records
            var testData = new Dictionary<string, string>();
            var insertStopwatch = Stopwatch.StartNew();
            for (int i = 0; i < 100; i++)
            {
                var key = $"range_key_{i:D3}";
                var value = $"range_value_{i}";
                testData[key] = value;
                await db.SetAsync(key, Encoding.UTF8.GetBytes(value));
            }
            insertStopwatch.Stop();
            Console.WriteLine($"✓ Inserted {testData.Count} sorted records in {insertStopwatch.ElapsedMilliseconds}ms");

            var flushStopwatch = Stopwatch.StartNew();
            await db.FlushAsync(); // Ensure data is in SSTables
            flushStopwatch.Stop();
            Console.WriteLine($"✓ Flushed to SSTables in {flushStopwatch.ElapsedMilliseconds}ms");

            // Test range queries - more comprehensive ranges
            var rangeTests = new[]
            {
                ("range_key_010", "range_key_020", 11), // 010 to 020 inclusive
                ("range_key_025", "range_key_035", 11), // 025 to 035 inclusive
                ("range_key_050", "range_key_055", 6),  // 050 to 055 inclusive
                ("range_key_070", "range_key_079", 10), // 070 to 079 inclusive
                ("range_key_090", "range_key_099", 10), // 090 to 099 inclusive
                ("range_key_000", "range_key_099", 100), // Full range
                ("range_key_000", "range_key_049", 50),  // First half
                ("range_key_050", "range_key_099", 50),  // Second half
            };

            Console.WriteLine($"Testing {rangeTests.Length} range queries...");

            var totalRangeTime = 0L;
            var passedRanges = 0;
            foreach (var (startKey, endKey, expectedCount) in rangeTests)
            {
                var rangeStopwatch = Stopwatch.StartNew();
                var rangeResults = new List<(string key, byte[] value)>();
                
                // Simulate range query by iterating through expected keys
                for (int i = 0; i < 100; i++)
                {
                    var key = $"range_key_{i:D3}";
                    if (string.Compare(key, startKey) >= 0 && string.Compare(key, endKey) <= 0)
                    {
                        var (found, value) = await db.GetAsync(key);
                        if (found)
                        {
                            rangeResults.Add((key, value));
                        }
                    }
                }
                rangeStopwatch.Stop();
                totalRangeTime += rangeStopwatch.ElapsedMilliseconds;

                var passed = rangeResults.Count == expectedCount;
                if (passed) passedRanges++;

                Console.WriteLine($"  Range [{startKey}, {endKey}]: Found {rangeResults.Count} records (Expected: {expectedCount}) - {(passed ? "✓ PASS" : "❌ FAIL")} - {rangeStopwatch.ElapsedMilliseconds}ms");
            }

            testStopwatch.Stop();
            Console.WriteLine($"✅ Range queries: {passedRanges}/{rangeTests.Length} passed");
            Console.WriteLine($"   Total time: {testStopwatch.ElapsedMilliseconds}ms, Range query time: {totalRangeTime}ms, Average range time: {totalRangeTime / (double)rangeTests.Length:F1}ms");
            Console.WriteLine();
        }

        private static async Task TransactionConsistencyTest(LSMTreeDB db)
        {
            var testStopwatch = Stopwatch.StartNew();
            Console.WriteLine("5. Transaction Consistency Test");
            Console.WriteLine("------------------------------");

            var key = "consistency_key";
            var concurrentTasks = new List<Task>();
            var updateCount = 50; // Increased from 10

            Console.WriteLine($"Testing concurrent consistency with {updateCount} threads...");

            var concurrentStopwatch = Stopwatch.StartNew();
            // Concurrent updates to the same key
            for (int i = 0; i < updateCount; i++)
            {
                var localI = i; // Capture for closure
                concurrentTasks.Add(Task.Run(async () =>
                {
                    var threadStopwatch = Stopwatch.StartNew();
                    await db.SetAsync(key, Encoding.UTF8.GetBytes($"update_{localI}"));
                    threadStopwatch.Stop();
                    if (localI % 10 == 0)
                    {
                        Console.WriteLine($"  Thread {localI} completed in {threadStopwatch.ElapsedMilliseconds}ms");
                    }
                }));
            }

            await Task.WhenAll(concurrentTasks);
            concurrentStopwatch.Stop();
            Console.WriteLine($"✓ All {updateCount} concurrent updates completed in {concurrentStopwatch.ElapsedMilliseconds}ms");

            // Verify that we have a valid final state
            var verifyStopwatch = Stopwatch.StartNew();
            var (found, finalValue) = await db.GetAsync(key);
            verifyStopwatch.Stop();
            var finalStr = found ? Encoding.UTF8.GetString(finalValue) : "NOT_FOUND";
            
            var passed = found && finalStr.StartsWith("update_");
            testStopwatch.Stop();
            
            Console.WriteLine($"✓ Final state verification in {verifyStopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"  Final state after concurrent updates: {finalStr}");
            Console.WriteLine($"✅ Consistency test: {(passed ? "PASS" : "FAIL")}");
            Console.WriteLine($"   Total time: {testStopwatch.ElapsedMilliseconds}ms, Average concurrent update rate: {updateCount * 1000.0 / concurrentStopwatch.ElapsedMilliseconds:F0} ops/sec");
            Console.WriteLine();
        }

        private static async Task DataIntegrityTest(LSMTreeDB db)
        {
            var testStopwatch = Stopwatch.StartNew();
            Console.WriteLine("6. Data Integrity Test");
            Console.WriteLine("---------------------");

            // Use a separate database instance to avoid conflicts
            var dbPath = Path.Combine(Environment.CurrentDirectory, "test_integrity_db");
            CleanupDirectory(dbPath);
            
            await using var integrityDb = await LSMTreeDB.OpenAsync(dbPath);

            var testData = new Dictionary<string, byte[]>();
            var random = new Random(42); // Fixed seed for reproducibility

            // Increased from 50 to 200 records
            var recordCount = 200;
            Console.WriteLine($"Testing data integrity with {recordCount} records...");

            // Generate test data with simple validation pattern
            var insertStopwatch = Stopwatch.StartNew();
            for (int i = 0; i < recordCount; i++)
            {
                var key = $"integrity_test_{i:D4}";
                var data = new byte[128]; // Increased data size
                random.NextBytes(data);
                
                // Add simple pattern for validation at the beginning
                var pattern = BitConverter.GetBytes(i);
                Array.Copy(pattern, 0, data, 0, Math.Min(pattern.Length, data.Length));
                
                testData[key] = data;
                await integrityDb.SetAsync(key, data);
                
                if (i > 0 && i % 50 == 0)
                {
                    Console.WriteLine($"  Inserted {i}/{recordCount} records...");
                }
            }
            insertStopwatch.Stop();
            Console.WriteLine($"✓ Inserted {recordCount} records in {insertStopwatch.ElapsedMilliseconds}ms ({recordCount * 1000.0 / insertStopwatch.ElapsedMilliseconds:F0} ops/sec)");

            // Test 1: Verify data exists immediately after writing
            Console.WriteLine("Testing data before flush/compaction...");
            var immediateStopwatch = Stopwatch.StartNew();
            var immediateTestCount = 0;
            foreach (var kvp in testData.Take(10))
            {
                var (found, value) = await integrityDb.GetAsync(kvp.Key);
                if (found && value.SequenceEqual(kvp.Value))
                {
                    immediateTestCount++;
                }
            }
            immediateStopwatch.Stop();
            Console.WriteLine($"✓ Before flush: {immediateTestCount}/10 test records found in {immediateStopwatch.ElapsedMilliseconds}ms");

            // Force flush and compaction
            var flushStopwatch = Stopwatch.StartNew();
            await integrityDb.FlushAsync();
            flushStopwatch.Stop();
            
            // Test 2: Verify data exists after flush but before compaction
            Console.WriteLine("Testing data after flush, before compaction...");
            var afterFlushStopwatch = Stopwatch.StartNew();
            var afterFlushCount = 0;
            foreach (var kvp in testData.Take(10))
            {
                var (found, value) = await integrityDb.GetAsync(kvp.Key);
                if (found && value.SequenceEqual(kvp.Value))
                {
                    afterFlushCount++;
                }
            }
            afterFlushStopwatch.Stop();
            Console.WriteLine($"✓ After flush: {afterFlushCount}/10 test records found in {afterFlushStopwatch.ElapsedMilliseconds}ms (flush took {flushStopwatch.ElapsedMilliseconds}ms)");
            
            try
            {
                Console.WriteLine("Starting compaction...");
                var compactStopwatch = Stopwatch.StartNew();
                await integrityDb.CompactAsync();
                compactStopwatch.Stop();
                Console.WriteLine($"✓ Compaction completed successfully in {compactStopwatch.ElapsedMilliseconds}ms");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Compaction failed: {ex.Message}");
            }
            
            // Test 3: Verify data exists after compaction
            Console.WriteLine("Testing data after compaction...");
            var afterCompactStopwatch = Stopwatch.StartNew();
            var afterCompactCount = 0;
            foreach (var kvp in testData.Take(10))
            {
                var (found, value) = await integrityDb.GetAsync(kvp.Key);
                if (found && value.SequenceEqual(kvp.Value))
                {
                    afterCompactCount++;
                }
            }
            afterCompactStopwatch.Stop();
            Console.WriteLine($"✓ After compaction: {afterCompactCount}/10 test records found in {afterCompactStopwatch.ElapsedMilliseconds}ms");

            // Final verification of all data
            Console.WriteLine("\n=== FINAL VERIFICATION ===");

            // Verify data integrity
            var verifyStopwatch = Stopwatch.StartNew();
            var verifyTasks = new List<Task<(bool, byte[])>>();
            foreach (var kvp in testData)
            {
                verifyTasks.Add(integrityDb.GetAsync(kvp.Key));
            }

            var results = await Task.WhenAll(verifyTasks);
            verifyStopwatch.Stop();
            
            var validCount = 0;
            var index = 0;

            foreach (var kvp in testData)
            {
                var (found, value) = results[index];
                
                if (found && value.SequenceEqual(kvp.Value))
                {
                    validCount++;
                }
                else if (!found && index < 3)
                {
                    Console.WriteLine($"❌ DEBUG: Key '{kvp.Key}' not found");
                }
                index++;
            }

            testStopwatch.Stop();
            var successRate = validCount * 100.0 / testData.Count;
            Console.WriteLine($"✅ Data integrity: {validCount}/{testData.Count} records verified ({successRate:F1}%)");
            Console.WriteLine($"   Total time: {testStopwatch.ElapsedMilliseconds}ms, Verification time: {verifyStopwatch.ElapsedMilliseconds}ms, Verification rate: {testData.Count * 1000.0 / verifyStopwatch.ElapsedMilliseconds:F0} ops/sec");
            Console.WriteLine();
        }

        private static async Task SimpleIntegrityTest()
        {
            var testStopwatch = Stopwatch.StartNew();
            Console.WriteLine("6b. Simple Integrity Test (No Compaction)");
            Console.WriteLine("------------------------------------------");

            var dbPath = Path.Combine(Environment.CurrentDirectory, "test_simple_integrity");
            CleanupDirectory(dbPath);
            
            await using var db = await LSMTreeDB.OpenAsync(dbPath);

            var testData = new Dictionary<string, string>();
            
            // Increased from 10 to 50 test records
            var recordCount = 50;
            Console.WriteLine($"Testing simple integrity with {recordCount} records (flush only, no compaction)...");
            
            // Write simple test data
            var insertStopwatch = Stopwatch.StartNew();
            for (int i = 0; i < recordCount; i++)
            {
                var key = $"simple_{i:D3}";
                var value = $"value_{i}_with_longer_content_for_testing";
                testData[key] = value;
                await db.SetAsync(key, Encoding.UTF8.GetBytes(value));
            }
            insertStopwatch.Stop();
            Console.WriteLine($"✓ Inserted {recordCount} records in {insertStopwatch.ElapsedMilliseconds}ms ({recordCount * 1000.0 / insertStopwatch.ElapsedMilliseconds:F0} ops/sec)");

            // Only flush, no compaction
            var flushStopwatch = Stopwatch.StartNew();
            await db.FlushAsync();
            flushStopwatch.Stop();
            Console.WriteLine($"✓ Flushed in {flushStopwatch.ElapsedMilliseconds}ms");

            // Verify all data
            var verifyStopwatch = Stopwatch.StartNew();
            var validCount = 0;
            foreach (var kvp in testData)
            {
                var (found, valueBytes) = await db.GetAsync(kvp.Key);
                if (found && Encoding.UTF8.GetString(valueBytes) == kvp.Value)
                {
                    validCount++;
                }
            }
            verifyStopwatch.Stop();

            testStopwatch.Stop();
            var successRate = validCount * 100.0 / testData.Count;
            Console.WriteLine($"✅ Simple integrity (flush only): {validCount}/{testData.Count} records verified ({successRate:F1}%)");
            Console.WriteLine($"   Total time: {testStopwatch.ElapsedMilliseconds}ms, Verification time: {verifyStopwatch.ElapsedMilliseconds}ms, Verification rate: {testData.Count * 1000.0 / verifyStopwatch.ElapsedMilliseconds:F0} ops/sec");
            Console.WriteLine();
        }

        private static async Task EdgeCaseTests(LSMTreeDB db)
        {
            var testStopwatch = Stopwatch.StartNew();
            Console.WriteLine("7. Edge Case Tests");
            Console.WriteLine("-----------------");

            var edgeCases = new List<(string description, Func<Task<bool>> test)>();

            // Empty value test
            edgeCases.Add(("Empty value", async () =>
            {
                var stopwatch = Stopwatch.StartNew();
                await db.SetAsync("empty_value_key", new byte[0]);
                var (found, value) = await db.GetAsync("empty_value_key");
                stopwatch.Stop();
                Console.WriteLine($"    Empty value test completed in {stopwatch.ElapsedMilliseconds}ms");
                return found && value.Length == 0;
            }));

            // Large key test
            edgeCases.Add(("Large key", async () =>
            {
                var stopwatch = Stopwatch.StartNew();
                var largeKey = new string('x', 1000);
                await db.SetAsync(largeKey, Encoding.UTF8.GetBytes("large_key_value"));
                var (found, value) = await db.GetAsync(largeKey);
                stopwatch.Stop();
                Console.WriteLine($"    Large key test (1000 chars) completed in {stopwatch.ElapsedMilliseconds}ms");
                return found && Encoding.UTF8.GetString(value) == "large_key_value";
            }));

            // Large value test
            edgeCases.Add(("Large value", async () =>
            {
                var stopwatch = Stopwatch.StartNew();
                var largeValue = new byte[10240]; // 10KB
                new Random(42).NextBytes(largeValue);
                await db.SetAsync("large_value_key", largeValue);
                var (found, value) = await db.GetAsync("large_value_key");
                stopwatch.Stop();
                Console.WriteLine($"    Large value test (10KB) completed in {stopwatch.ElapsedMilliseconds}ms");
                return found && value.SequenceEqual(largeValue);
            }));

            // Binary data test
            edgeCases.Add(("Binary data", async () =>
            {
                var stopwatch = Stopwatch.StartNew();
                var binaryData = new byte[] { 0, 1, 2, 255, 254, 253, 128, 127, 64, 32, 16, 8, 4, 2, 1 };
                await db.SetAsync("binary_key", binaryData);
                var (found, value) = await db.GetAsync("binary_key");
                stopwatch.Stop();
                Console.WriteLine($"    Binary data test completed in {stopwatch.ElapsedMilliseconds}ms");
                return found && value.SequenceEqual(binaryData);
            }));

            // Unicode key test
            edgeCases.Add(("Unicode key", async () =>
            {
                var stopwatch = Stopwatch.StartNew();
                var unicodeKey = "🔑测试키中文日本語🚀";
                await db.SetAsync(unicodeKey, Encoding.UTF8.GetBytes("unicode_value_with_emojis_🎉"));
                var (found, value) = await db.GetAsync(unicodeKey);
                stopwatch.Stop();
                Console.WriteLine($"    Unicode key test completed in {stopwatch.ElapsedMilliseconds}ms");
                return found && Encoding.UTF8.GetString(value) == "unicode_value_with_emojis_🎉";
            }));

            // Non-existent key test
            edgeCases.Add(("Non-existent key", async () =>
            {
                var stopwatch = Stopwatch.StartNew();
                var (found, _) = await db.GetAsync("definitely_not_exists_" + Guid.NewGuid());
                stopwatch.Stop();
                Console.WriteLine($"    Non-existent key test completed in {stopwatch.ElapsedMilliseconds}ms");
                return !found;
            }));

            // Null key handling test
            edgeCases.Add(("Null key handling", async () =>
            {
                var stopwatch = Stopwatch.StartNew();
                try
                {
                    await db.SetAsync(null!, Encoding.UTF8.GetBytes("null_key_value"));
                    stopwatch.Stop();
                    Console.WriteLine($"    Null key test completed in {stopwatch.ElapsedMilliseconds}ms");
                    return false; // Should throw exception
                }
                catch (ArgumentNullException)
                {
                    stopwatch.Stop();
                    Console.WriteLine($"    Null key test completed in {stopwatch.ElapsedMilliseconds}ms (expected exception)");
                    return true; // Expected behavior
                }
                catch
                {
                    stopwatch.Stop();
                    return false; // Wrong exception type
                }
            }));

            // Special characters test
            edgeCases.Add(("Special characters", async () =>
            {
                var stopwatch = Stopwatch.StartNew();
                var specialKey = "key!@#$%^&*()_+-=[]{}|;':\",./<>?`~";
                var specialValue = "value!@#$%^&*()_+-=[]{}|;':\",./<>?`~";
                await db.SetAsync(specialKey, Encoding.UTF8.GetBytes(specialValue));
                var (found, value) = await db.GetAsync(specialKey);
                stopwatch.Stop();
                Console.WriteLine($"    Special characters test completed in {stopwatch.ElapsedMilliseconds}ms");
                return found && Encoding.UTF8.GetString(value) == specialValue;
            }));

            Console.WriteLine($"Testing {edgeCases.Count} edge cases...");

            // Execute edge case tests
            var passedTests = 0;
            var totalTime = 0L;
            foreach (var (description, test) in edgeCases)
            {
                var caseStopwatch = Stopwatch.StartNew();
                try
                {
                    var result = await test();
                    caseStopwatch.Stop();
                    totalTime += caseStopwatch.ElapsedMilliseconds;
                    if (result)
                    {
                        passedTests++;
                        Console.WriteLine($"  ✓ {description}: PASS ({caseStopwatch.ElapsedMilliseconds}ms)");
                    }
                    else
                    {
                        Console.WriteLine($"  ❌ {description}: FAIL ({caseStopwatch.ElapsedMilliseconds}ms)");
                    }
                }
                catch (Exception ex)
                {
                    caseStopwatch.Stop();
                    totalTime += caseStopwatch.ElapsedMilliseconds;
                    Console.WriteLine($"  ❌ {description}: FAIL (Exception: {ex.Message}) ({caseStopwatch.ElapsedMilliseconds}ms)");
                }
            }

            testStopwatch.Stop();
            Console.WriteLine($"✅ Edge cases: {passedTests}/{edgeCases.Count} passed");
            Console.WriteLine($"   Total time: {testStopwatch.ElapsedMilliseconds}ms, Average test time: {totalTime / (double)edgeCases.Count:F1}ms");
            Console.WriteLine();
        }

        private static int ComputeSimpleChecksum(byte[] data)
        {
            int checksum = 0;
            foreach (byte b in data)
            {
                checksum = (checksum + b) % int.MaxValue;
            }
            return checksum;
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
