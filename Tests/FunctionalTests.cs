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
            Console.WriteLine("LSM-Tree Functional Tests");
            Console.WriteLine("=========================");

            var dbPath = Path.Combine(Environment.CurrentDirectory, "test_functional_db");
            CleanupDirectory(dbPath);

            using var db = await LSMTreeDB.OpenAsync(dbPath);

            await BasicOperationsTest(db);
            await UpdateOperationsTest(db);
            await DeleteOperationsTest(db);
            await RangeQueryTest(db);
            await TransactionConsistencyTest(db);
            await DataIntegrityTest(db);
            await EdgeCaseTests(db);

            Console.WriteLine("\nFunctional tests completed!");
        }

        private static async Task BasicOperationsTest(LSMTreeDB db)
        {
            Console.WriteLine("\n1. Basic Operations Test");
            Console.WriteLine("-----------------------");

            // Test basic Put/Get operations
            var testCases = new[]
            {
                ("key1", "value1"),
                ("key2", "value2"),
                ("key3", "value3"),
                ("", "empty_key_value"),
                ("very_long_key_" + new string('x', 1000), "long_key_value"),
                ("key_with_special_chars_!@#$%^&*()", "special_value")
            };

            // Insert all test cases
            var insertTasks = new List<Task>();
            foreach (var (key, value) in testCases)
            {
                insertTasks.Add(db.SetAsync(key, Encoding.UTF8.GetBytes(value)));
            }
            await Task.WhenAll(insertTasks);

            // Verify all test cases
            var verifyTasks = new List<Task<(bool, byte[])>>();
            foreach (var (key, _) in testCases)
            {
                verifyTasks.Add(db.GetAsync(key));
            }
            var results = await Task.WhenAll(verifyTasks);

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
                    Console.WriteLine($"FAIL: Key '{testCases[i].Item1}' - Expected: '{expectedValue}', Got: '{(found ? Encoding.UTF8.GetString(value) : "NOT_FOUND")}'");
                }
            }

            Console.WriteLine($"Basic operations: {passCount}/{testCases.Length} tests passed");
        }

        private static async Task UpdateOperationsTest(LSMTreeDB db)
        {
            Console.WriteLine("\n2. Update Operations Test");
            Console.WriteLine("------------------------");

            var key = "update_test_key";
            var versions = new[] { "version1", "version2", "version3", "final_version" };

            // Update the same key multiple times
            for (int i = 0; i < versions.Length; i++)
            {
                await db.SetAsync(key, Encoding.UTF8.GetBytes(versions[i]));
                
                var (found, value) = await db.GetAsync(key);
                var actualValue = found ? Encoding.UTF8.GetString(value) : "NOT_FOUND";
                
                if (actualValue == versions[i])
                {
                    Console.WriteLine($"Update {i + 1}: PASS - Value is '{actualValue}'");
                }
                else
                {
                    Console.WriteLine($"Update {i + 1}: FAIL - Expected '{versions[i]}', Got '{actualValue}'");
                }
            }

            // Force flush and verify persistence
            await db.FlushAsync();
            var (finalFound, finalValue) = await db.GetAsync(key);
            var finalActual = finalFound ? Encoding.UTF8.GetString(finalValue) : "NOT_FOUND";
            
            Console.WriteLine($"After flush: {(finalActual == versions.Last() ? "PASS" : "FAIL")} - Value is '{finalActual}'");
        }

        private static async Task DeleteOperationsTest(LSMTreeDB db)
        {
            Console.WriteLine("\n3. Delete Operations Test");
            Console.WriteLine("------------------------");

            var testKeys = new[] { "delete1", "delete2", "delete3" };
            var testValues = new[] { "value1", "value2", "value3" };

            // Insert test data
            var insertTasks = new List<Task>();
            for (int i = 0; i < testKeys.Length; i++)
            {
                insertTasks.Add(db.SetAsync(testKeys[i], Encoding.UTF8.GetBytes(testValues[i])));
            }
            await Task.WhenAll(insertTasks);

            // Verify insertion
            for (int i = 0; i < testKeys.Length; i++)
            {
                var (found, value) = await db.GetAsync(testKeys[i]);
                Console.WriteLine($"Before delete - {testKeys[i]}: {(found ? "FOUND" : "NOT_FOUND")}");
            }

            // Delete keys one by one
            for (int i = 0; i < testKeys.Length; i++)
            {
                await db.DeleteAsync(testKeys[i]);
                
                var (found, _) = await db.GetAsync(testKeys[i]);
                Console.WriteLine($"After deleting {testKeys[i]}: {(found ? "STILL_FOUND (FAIL)" : "NOT_FOUND (PASS)")}");
            }

            // Test deleting non-existent key
            await db.DeleteAsync("non_existent_key");
            Console.WriteLine("Delete non-existent key: PASS (no exception)");

            // Force flush and verify deletes persist
            await db.FlushAsync();
            for (int i = 0; i < testKeys.Length; i++)
            {
                var (found, _) = await db.GetAsync(testKeys[i]);
                Console.WriteLine($"After flush - {testKeys[i]}: {(found ? "STILL_FOUND (FAIL)" : "NOT_FOUND (PASS)")}");
            }
        }

        private static async Task RangeQueryTest(LSMTreeDB db)
        {
            Console.WriteLine("\n4. Range Query Test");
            Console.WriteLine("------------------");

            // Insert sorted test data
            var testData = new Dictionary<string, string>();
            for (int i = 0; i < 100; i++)
            {
                var key = $"range_key_{i:D3}";
                var value = $"range_value_{i}";
                testData[key] = value;
                await db.SetAsync(key, Encoding.UTF8.GetBytes(value));
            }

            await db.FlushAsync(); // Ensure data is in SSTables

            // Test range queries
            var rangeTests = new[]
            {
                ("range_key_010", "range_key_020", 11), // 010 to 020 inclusive
                ("range_key_050", "range_key_055", 6),  // 050 to 055 inclusive
                ("range_key_090", "range_key_099", 10), // 090 to 099 inclusive
                ("range_key_000", "range_key_099", 100), // Full range
            };

            foreach (var (startKey, endKey, expectedCount) in rangeTests)
            {
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

                Console.WriteLine($"Range [{startKey}, {endKey}]: Found {rangeResults.Count} records (Expected: {expectedCount}) - {(rangeResults.Count == expectedCount ? "PASS" : "FAIL")}");
            }
        }

        private static async Task TransactionConsistencyTest(LSMTreeDB db)
        {
            Console.WriteLine("\n5. Transaction Consistency Test");
            Console.WriteLine("------------------------------");

            var key = "consistency_key";
            var concurrentTasks = new List<Task>();
            var updateCount = 100;

            // Concurrent updates to the same key
            for (int i = 0; i < updateCount; i++)
            {
                var localI = i; // Capture for closure
                concurrentTasks.Add(Task.Run(async () =>
                {
                    await db.SetAsync(key, Encoding.UTF8.GetBytes($"update_{localI}"));
                }));
            }

            await Task.WhenAll(concurrentTasks);

            // Verify that we have a valid final state
            var (found, finalValue) = await db.GetAsync(key);
            var finalStr = found ? Encoding.UTF8.GetString(finalValue) : "NOT_FOUND";
            
            Console.WriteLine($"Final state after concurrent updates: {finalStr}");
            Console.WriteLine($"Consistency test: {(found && finalStr.StartsWith("update_") ? "PASS" : "FAIL")}");
        }

        private static async Task DataIntegrityTest(LSMTreeDB db)
        {
            Console.WriteLine("\n6. Data Integrity Test");
            Console.WriteLine("---------------------");

            var testData = new Dictionary<string, byte[]>();
            var random = new Random(42); // Fixed seed for reproducibility

            // Generate test data with checksums
            for (int i = 0; i < 1000; i++)
            {
                var key = $"integrity_{i:D4}";
                var data = new byte[256];
                random.NextBytes(data);
                
                // Add checksum to data
                var checksum = ComputeSimpleChecksum(data);
                var valueWithChecksum = new byte[data.Length + 4];
                Array.Copy(data, 0, valueWithChecksum, 0, data.Length);
                Array.Copy(BitConverter.GetBytes(checksum), 0, valueWithChecksum, data.Length, 4);
                
                testData[key] = valueWithChecksum;
                await db.SetAsync(key, valueWithChecksum);
            }

            // Force flush and compaction
            await db.FlushAsync();
            await db.CompactAsync();

            // Verify data integrity
            var verifyTasks = new List<Task<(bool, byte[])>>();
            foreach (var kvp in testData)
            {
                verifyTasks.Add(db.GetAsync(kvp.Key));
            }

            var results = await Task.WhenAll(verifyTasks);
            var validCount = 0;
            var index = 0;

            foreach (var kvp in testData)
            {
                var (found, value) = results[index++];
                
                if (found && value.Length == kvp.Value.Length)
                {
                    var originalData = new byte[value.Length - 4];
                    Array.Copy(value, 0, originalData, 0, originalData.Length);
                    
                    var storedChecksum = BitConverter.ToInt32(value, value.Length - 4);
                    var computedChecksum = ComputeSimpleChecksum(originalData);
                    
                    if (storedChecksum == computedChecksum && value.SequenceEqual(kvp.Value))
                    {
                        validCount++;
                    }
                }
            }

            Console.WriteLine($"Data integrity: {validCount}/{testData.Count} records verified ({validCount * 100.0 / testData.Count:F1}%)");
        }

        private static async Task EdgeCaseTests(LSMTreeDB db)
        {
            Console.WriteLine("\n7. Edge Case Tests");
            Console.WriteLine("-----------------");

            var edgeCases = new List<(string description, Func<Task<bool>> test)>();

            // Empty value test
            edgeCases.Add(("Empty value", async () =>
            {
                await db.SetAsync("empty_value_key", new byte[0]);
                var (found, value) = await db.GetAsync("empty_value_key");
                return found && value.Length == 0;
            }));

            // Large key test
            edgeCases.Add(("Large key", async () =>
            {
                var largeKey = new string('x', 10000);
                await db.SetAsync(largeKey, Encoding.UTF8.GetBytes("large_key_value"));
                var (found, value) = await db.GetAsync(largeKey);
                return found && Encoding.UTF8.GetString(value) == "large_key_value";
            }));

            // Binary data test
            edgeCases.Add(("Binary data", async () =>
            {
                var binaryData = new byte[] { 0, 1, 2, 255, 254, 253, 128 };
                await db.SetAsync("binary_key", binaryData);
                var (found, value) = await db.GetAsync("binary_key");
                return found && value.SequenceEqual(binaryData);
            }));

            // Unicode key test
            edgeCases.Add(("Unicode key", async () =>
            {
                var unicodeKey = "🔑测试키";
                await db.SetAsync(unicodeKey, Encoding.UTF8.GetBytes("unicode_value"));
                var (found, value) = await db.GetAsync(unicodeKey);
                return found && Encoding.UTF8.GetString(value) == "unicode_value";
            }));

            // Non-existent key test
            edgeCases.Add(("Non-existent key", async () =>
            {
                var (found, _) = await db.GetAsync("definitely_not_exists_" + Guid.NewGuid());
                return !found;
            }));

            // Execute edge case tests
            foreach (var (description, test) in edgeCases)
            {
                try
                {
                    var result = await test();
                    Console.WriteLine($"{description}: {(result ? "PASS" : "FAIL")}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{description}: FAIL (Exception: {ex.Message})");
                }
            }
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
