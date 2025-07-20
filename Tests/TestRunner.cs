using System;
using System.Threading.Tasks;

namespace LSMTree.Tests
{
    public class TestRunner
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("LSM-Tree Heavy Test Suite");
            Console.WriteLine("========================");
            Console.WriteLine($"Started at: {DateTime.Now}");
            Console.WriteLine();

            try
            {
                if (args.Length > 0)
                {
                    switch (args[0].ToLower())
                    {
                        case "functional":
                            await FunctionalTests.RunAllAsync();
                            break;
                        case "performance":
                            await PerformanceTests.RunAllAsync();
                            break;
                        case "stress":
                            await StressTests.RunAllAsync();
                            break;
                        case "bloom":
                            BloomFilterBenchmark.RunBenchmark();
                            break;
                        case "all":
                        default:
                            await RunAllTests();
                            break;
                    }
                }
                else
                {
                    await RunAllTests();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\nTest execution failed: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                Environment.Exit(1);
            }

            Console.WriteLine($"\nCompleted at: {DateTime.Now}");
            Console.WriteLine("\nTest suite finished successfully!");
        }

        private static async Task RunAllTests()
        {
            Console.WriteLine("Running complete test suite...");
            Console.WriteLine();

            await FunctionalTests.RunAllAsync();
            Console.WriteLine();
            
            await PerformanceTests.RunAllAsync();
            Console.WriteLine();
            
            await StressTests.RunAllAsync();
        }
    }
}
