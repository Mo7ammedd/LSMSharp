using System.Text;
using LSMTree;

namespace LSMTree.Tests
{
    public class SimpleTest
    {
        public static async Task RunAsync()
        {
            Console.WriteLine("Simple LSM-Tree Test");
            Console.WriteLine("====================");

            // Create a simple test
            var dbPath = Path.Combine(Environment.CurrentDirectory, "test_db");
            if (Directory.Exists(dbPath))
            {
                Directory.Delete(dbPath, true);
            }

            using var db = await LSMTreeDB.OpenAsync(dbPath);

            // Test 1: Basic Set/Get
            Console.WriteLine("\nTest 1: Basic Set/Get");
            await db.SetAsync("test1", Encoding.UTF8.GetBytes("value1"));
            var (found1, value1) = await db.GetAsync("test1");
            Console.WriteLine($"test1 = {(found1 ? Encoding.UTF8.GetString(value1) : "Not found")}");

            // Test 2: Update
            Console.WriteLine("\nTest 2: Update");
            await db.SetAsync("test1", Encoding.UTF8.GetBytes("updated_value1"));
            var (found2, value2) = await db.GetAsync("test1");
            Console.WriteLine($"test1 (updated) = {(found2 ? Encoding.UTF8.GetString(value2) : "Not found")}");

            // Test 3: Multiple keys
            Console.WriteLine("\nTest 3: Multiple keys");
            await db.SetAsync("apple", Encoding.UTF8.GetBytes("fruit"));
            await db.SetAsync("banana", Encoding.UTF8.GetBytes("yellow"));
            await db.SetAsync("cherry", Encoding.UTF8.GetBytes("red"));

            var (foundA, valueA) = await db.GetAsync("apple");
            var (foundB, valueB) = await db.GetAsync("banana");
            var (foundC, valueC) = await db.GetAsync("cherry");

            Console.WriteLine($"apple = {(foundA ? Encoding.UTF8.GetString(valueA) : "Not found")}");
            Console.WriteLine($"banana = {(foundB ? Encoding.UTF8.GetString(valueB) : "Not found")}");
            Console.WriteLine($"cherry = {(foundC ? Encoding.UTF8.GetString(valueC) : "Not found")}");

            // Test 4: Delete
            Console.WriteLine("\nTest 4: Delete");
            await db.DeleteAsync("banana");
            var (foundD, valueD) = await db.GetAsync("banana");
            Console.WriteLine($"banana (after delete) = {(foundD ? Encoding.UTF8.GetString(valueD) : "Not found")}");

            // Test 5: Force flush and read
            Console.WriteLine("\nTest 5: Force flush and read");
            await db.FlushAsync();
            
            var (foundE, valueE) = await db.GetAsync("apple");
            var (foundF, valueF) = await db.GetAsync("cherry");
            Console.WriteLine($"apple (after flush) = {(foundE ? Encoding.UTF8.GetString(valueE) : "Not found")}");
            Console.WriteLine($"cherry (after flush) = {(foundF ? Encoding.UTF8.GetString(valueF) : "Not found")}");

            Console.WriteLine("\nTest completed!");
        }
    }
}
