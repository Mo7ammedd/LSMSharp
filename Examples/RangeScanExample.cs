using System;
using System.Text;
using System.Threading.Tasks;
using LSMTree;
using LSMTree.Core;

namespace LSMTree.Examples
{
    /// <summary>
    /// Example demonstrating range scan functionality in LSM-Tree.
    /// </summary>
    public class RangeScanExample
    {
        public static async Task RunAsync()
        {
            Console.WriteLine("=== Range Scan Example ===\n");

            var dbPath = "./example_rangescan_db";
            if (System.IO.Directory.Exists(dbPath))
            {
                System.IO.Directory.Delete(dbPath, true);
            }

            // Create database with default configuration
            await using var db = await LSMTreeDB.OpenAsync(dbPath);

            // Insert sample data
            Console.WriteLine("Inserting sample data...");
            for (int i = 1; i <= 20; i++)
            {
                var key = $"key_{i:D3}";
                var value = Encoding.UTF8.GetBytes($"Value for {key}");
                await db.SetAsync(key, value);
            }

            Console.WriteLine("Inserted 20 keys (key_001 to key_020)\n");

            // Perform a range scan
            Console.WriteLine("Range scan from key_005 to key_010:");
            Console.WriteLine("------------------------------------");
            
            await foreach (var (key, value) in db.RangeAsync("key_005", "key_010"))
            {
                Console.WriteLine($"  {key} => {Encoding.UTF8.GetString(value)}");
            }

            Console.WriteLine("\nRange scan from key_015 to key_020:");
            Console.WriteLine("------------------------------------");
            
            await foreach (var (key, value) in db.RangeAsync("key_015", "key_020"))
            {
                Console.WriteLine($"  {key} => {Encoding.UTF8.GetString(value)}");
            }

            // Demonstrate range scan with updates
            Console.WriteLine("\nUpdating key_007 and deleting key_008...");
            await db.SetAsync("key_007", Encoding.UTF8.GetBytes("Updated value for key_007"));
            await db.DeleteAsync("key_008");

            Console.WriteLine("\nRange scan from key_005 to key_010 (after updates):");
            Console.WriteLine("-----------------------------------------------------");
            
            await foreach (var (key, value) in db.RangeAsync("key_005", "key_010"))
            {
                Console.WriteLine($"  {key} => {Encoding.UTF8.GetString(value)}");
            }

            Console.WriteLine("\nRange scan example completed!");
        }
    }
}
