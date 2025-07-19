using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using LSMTree.BloomFilter;

namespace LSMTree.Tests
{
    public class BloomFilterBenchmark
    {
        public static void RunBenchmark()
        {
            var totalStopwatch = Stopwatch.StartNew();
            Console.WriteLine("Bloom Filter Performance Benchmark");
            Console.WriteLine("===================================");
            Console.WriteLine($"Started at: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            Console.WriteLine();

            // Test different configurations
            var configurations = new[]
            {
                (elements: 1000, fpr: 0.01, name: "Small Dataset"),
                (elements: 10000, fpr: 0.01, name: "Medium Dataset"),
                (elements: 100000, fpr: 0.01, name: "Large Dataset"),
                (elements: 10000, fpr: 0.001, name: "Low FPR"),
                (elements: 10000, fpr: 0.1, name: "High FPR")
            };

            var results = new List<BenchmarkResult>();

            foreach (var (elements, fpr, name) in configurations)
            {
                Console.WriteLine($"{name} Configuration");
                Console.WriteLine(new string('-', 50));
                var result = BenchmarkConfiguration(elements, fpr);
                results.Add(result);
                Console.WriteLine();
            }

            totalStopwatch.Stop();
            PrintSummary(results, totalStopwatch.Elapsed);
        }

        private static BenchmarkResult BenchmarkConfiguration(int elements, double fpr)
        {
            Console.WriteLine($"Elements: {elements:N0}, False Positive Rate: {fpr:P}");
            
            var result = new BenchmarkResult
            {
                Elements = elements,
                FalsePositiveRate = fpr
            };

            // Create bloom filter
            var createStopwatch = Stopwatch.StartNew();
            var filter = new LSMTree.BloomFilter.BloomFilter(elements, fpr);
            createStopwatch.Stop();
            
            result.BitArraySize = filter.Size;
            result.HashFunctionCount = filter.HashFunctionCount;
            result.CreateTimeNs = createStopwatch.Elapsed.TotalNanoseconds;
            
            Console.WriteLine($"   Created in: {result.CreateTimeNs:F0} ns");
            Console.WriteLine($"   Bit array size: {filter.Size:N0} bits ({filter.Size / 8.0 / 1024:F2} KB)");
            Console.WriteLine($"   Hash functions: {filter.HashFunctionCount}");
            Console.WriteLine($"   Bits per element: {(double)filter.Size / elements:F1}");
            
            // Generate test keys
            var keys = new List<string>();
            var keyGenStopwatch = Stopwatch.StartNew();
            for (int i = 0; i < elements; i++)
            {
                keys.Add($"key_{i:D8}_{Guid.NewGuid():N}");
            }
            keyGenStopwatch.Stop();
            Console.WriteLine($"   Generated {elements:N0} keys in {keyGenStopwatch.ElapsedMilliseconds}ms");

            // Benchmark Add operations
            Console.WriteLine($"   Benchmarking Add operations...");
            var addStopwatch = Stopwatch.StartNew();
            foreach (var key in keys)
            {
                filter.Add(key);
            }
            addStopwatch.Stop();

            result.AddNsPerOp = addStopwatch.Elapsed.TotalNanoseconds / elements;
            result.AddOpsPerSec = elements * 1000.0 / addStopwatch.ElapsedMilliseconds;
            Console.WriteLine($"      Time: {result.AddNsPerOp:F1} ns/op");
            Console.WriteLine($"      Rate: {result.AddOpsPerSec:F0} ops/sec");

            // Benchmark Contains operations (positive hits)
            Console.WriteLine($"   Benchmarking Contains (positive lookups)...");
            var containsStopwatch = Stopwatch.StartNew();
            int hits = 0;
            foreach (var key in keys)
            {
                if (filter.Contains(key)) hits++;
            }
            containsStopwatch.Stop();

            result.ContainsHitNsPerOp = containsStopwatch.Elapsed.TotalNanoseconds / elements;
            result.ContainsHitOpsPerSec = elements * 1000.0 / containsStopwatch.ElapsedMilliseconds;
            result.HitRate = hits * 100.0 / elements;
            Console.WriteLine($"      Time: {result.ContainsHitNsPerOp:F1} ns/op");
            Console.WriteLine($"      Rate: {result.ContainsHitOpsPerSec:F0} ops/sec");
            Console.WriteLine($"      Hit rate: {result.HitRate:F1}% ({hits:N0}/{elements:N0})");

            // Benchmark Contains operations (negative lookups)
            Console.WriteLine($"   Benchmarking Contains (negative lookups)...");
            var negativeKeys = new List<string>();
            for (int i = 0; i < Math.Min(elements, 10000); i++) // Limit to 10k for large datasets
            {
                negativeKeys.Add($"negative_key_{i:D8}_{Guid.NewGuid():N}");
            }

            var negativeLookupStopwatch = Stopwatch.StartNew();
            int falsePositives = 0;
            foreach (var key in negativeKeys)
            {
                if (filter.Contains(key)) falsePositives++;
            }
            negativeLookupStopwatch.Stop();

            result.ContainsMissNsPerOp = negativeLookupStopwatch.Elapsed.TotalNanoseconds / negativeKeys.Count;
            result.ContainsMissOpsPerSec = negativeKeys.Count * 1000.0 / negativeLookupStopwatch.ElapsedMilliseconds;
            result.ActualFalsePositiveRate = falsePositives * 100.0 / negativeKeys.Count;
            Console.WriteLine($"      Time: {result.ContainsMissNsPerOp:F1} ns/op");
            Console.WriteLine($"      Rate: {result.ContainsMissOpsPerSec:F0} ops/sec");
            Console.WriteLine($"      False positive rate: {result.ActualFalsePositiveRate:F3}% ({falsePositives:N0}/{negativeKeys.Count:N0})");

            // Benchmark serialization
            Console.WriteLine($"   Benchmarking serialization...");
            var serializeStopwatch = Stopwatch.StartNew();
            var serialized = filter.Serialize();
            serializeStopwatch.Stop();
            
            result.SerializeTimeNs = serializeStopwatch.Elapsed.TotalNanoseconds;
            result.SerializedSizeBytes = serialized.Length;
            Console.WriteLine($"      Time: {result.SerializeTimeNs:F0} ns");
            Console.WriteLine($"      Size: {serialized.Length:N0} bytes ({serialized.Length / 1024.0:F2} KB)");

            // Benchmark deserialization
            Console.WriteLine($"   Benchmarking deserialization...");
            var newFilter = new LSMTree.BloomFilter.BloomFilter(elements, fpr);
            var deserializeStopwatch = Stopwatch.StartNew();
            newFilter.Deserialize(serialized);
            deserializeStopwatch.Stop();
            
            result.DeserializeTimeNs = deserializeStopwatch.Elapsed.TotalNanoseconds;
            Console.WriteLine($"      Time: {result.DeserializeTimeNs:F0} ns");

            // Verify deserialized filter works correctly
            var verifyCount = 0;
            foreach (var key in keys.Take(100)) // Test first 100 keys
            {
                if (newFilter.Contains(key)) verifyCount++;
            }
            Console.WriteLine($"      Verification: {verifyCount}/100 keys found after deserialization");

            return result;
        }

        private static void PrintSummary(List<BenchmarkResult> results, TimeSpan totalTime)
        {
            Console.WriteLine("================================================================");
            Console.WriteLine("                      BENCHMARK SUMMARY                        ");
            Console.WriteLine("================================================================");
            Console.WriteLine();

            // Performance comparison table
            Console.WriteLine("Performance Comparison:");
            Console.WriteLine($"{"Configuration",-20} {"Elements",-10} {"Add ns/op",-12} {"Contains ns/op",-15} {"FPR Actual",-12}");
            Console.WriteLine(new string('-', 80));
            
            foreach (var result in results)
            {
                var config = $"{result.Elements:N0} @ {result.FalsePositiveRate:P}";
                Console.WriteLine($"{config,-20} {result.Elements,-10:N0} {result.AddNsPerOp,-12:F1} {result.ContainsHitNsPerOp,-15:F1} {result.ActualFalsePositiveRate,-12:F3}%");
            }

            Console.WriteLine();
            Console.WriteLine("Best Performance:");
            var fastestAdd = results.OrderBy(r => r.AddNsPerOp).First();
            var fastestContains = results.OrderBy(r => r.ContainsHitNsPerOp).First();
            var lowestFPR = results.OrderBy(r => Math.Abs(r.ActualFalsePositiveRate - r.FalsePositiveRate * 100)).First();

            Console.WriteLine($"   Fastest Add: {fastestAdd.AddNsPerOp:F1} ns/op ({fastestAdd.Elements:N0} elements)");
            Console.WriteLine($"   Fastest Contains: {fastestContains.ContainsHitNsPerOp:F1} ns/op ({fastestContains.Elements:N0} elements)");
            Console.WriteLine($"   Most Accurate FPR: {lowestFPR.ActualFalsePositiveRate:F3}% (target: {lowestFPR.FalsePositiveRate:P})");

            Console.WriteLine();
            Console.WriteLine("Scaling Analysis:");
            var small = results.FirstOrDefault(r => r.Elements == 1000);
            var large = results.FirstOrDefault(r => r.Elements == 100000);
            if (small != null && large != null)
            {
                var addScaling = large.AddNsPerOp / small.AddNsPerOp;
                var containsScaling = large.ContainsHitNsPerOp / small.ContainsHitNsPerOp;
                Console.WriteLine($"   100x data size scaling:");
                Console.WriteLine($"      Add: {addScaling:F2}x slower ({small.AddNsPerOp:F1} -> {large.AddNsPerOp:F1} ns/op)");
                Console.WriteLine($"      Contains: {containsScaling:F2}x slower ({small.ContainsHitNsPerOp:F1} -> {large.ContainsHitNsPerOp:F1} ns/op)");
            }

            Console.WriteLine();
            Console.WriteLine($"Total benchmark time: {totalTime.TotalSeconds:F2}s");
            Console.WriteLine($"Completed at: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");

            // Memory efficiency analysis
            Console.WriteLine();
            Console.WriteLine("Memory Efficiency:");
            foreach (var result in results.Take(3)) // Show first 3 configurations
            {
                var bitsPerElement = (double)result.BitArraySize / result.Elements;
                var bytesPerElement = bitsPerElement / 8.0;
                Console.WriteLine($"   {result.Elements:N0} elements: {bitsPerElement:F1} bits/element ({bytesPerElement:F2} bytes/element)");
            }
        }

        private class BenchmarkResult
        {
            public int Elements { get; set; }
            public double FalsePositiveRate { get; set; }
            public int BitArraySize { get; set; }
            public int HashFunctionCount { get; set; }
            public double CreateTimeNs { get; set; }
            public double AddNsPerOp { get; set; }
            public double AddOpsPerSec { get; set; }
            public double ContainsHitNsPerOp { get; set; }
            public double ContainsHitOpsPerSec { get; set; }
            public double ContainsMissNsPerOp { get; set; }
            public double ContainsMissOpsPerSec { get; set; }
            public double HitRate { get; set; }
            public double ActualFalsePositiveRate { get; set; }
            public double SerializeTimeNs { get; set; }
            public double DeserializeTimeNs { get; set; }
            public int SerializedSizeBytes { get; set; }
        }
    }
}
