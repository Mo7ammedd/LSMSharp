using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using LSMTree.Core;

namespace LSMTree.BloomFilter
{
    public class BloomFilter : IBloomFilter
    {
        private readonly BitArray _bitArray;
        private readonly int _hashFunctionCount;
        private readonly double _expectedFalsePositiveRate;

        public int Size => _bitArray.Length;
        public int HashFunctionCount => _hashFunctionCount;

        public BloomFilter(int expectedElements, double falsePositiveRate = 0.01)
        {
            if (expectedElements <= 0)
                throw new ArgumentException("Expected elements must be positive", nameof(expectedElements));
            
            if (falsePositiveRate <= 0 || falsePositiveRate >= 1)
                throw new ArgumentException("False positive rate must be between 0 and 1", nameof(falsePositiveRate));

            _expectedFalsePositiveRate = falsePositiveRate;
            
            // Calculate optimal bit array size: m = -(n * ln(p)) / (ln(2)^2)
            int bitArraySize = (int)Math.Ceiling(-expectedElements * Math.Log(falsePositiveRate) / Math.Pow(Math.Log(2), 2));
            _bitArray = new BitArray(bitArraySize);

            // Calculate optimal number of hash functions: k = (m/n) * ln(2)
            _hashFunctionCount = Math.Max(1, (int)Math.Round(bitArraySize * Math.Log(2) / expectedElements));
        }

        public BloomFilter(int bitArraySize, int hashFunctionCount)
        {
            if (bitArraySize <= 0)
                throw new ArgumentException("Bit array size must be positive", nameof(bitArraySize));
            
            if (hashFunctionCount <= 0)
                throw new ArgumentException("Hash function count must be positive", nameof(hashFunctionCount));

            _bitArray = new BitArray(bitArraySize);
            _hashFunctionCount = hashFunctionCount;
            _expectedFalsePositiveRate = 0.01; // Default value
        }

        public void Add(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentException("Key cannot be null or empty", nameof(key));

            var hashes = GetHashes(key);
            for (int i = 0; i < _hashFunctionCount; i++)
            {
                int index = (int)(Math.Abs(hashes[i]) % _bitArray.Length);
                _bitArray[index] = true;
            }
        }

        public bool Contains(string key)
        {
            if (string.IsNullOrEmpty(key))
                return false;

            var hashes = GetHashes(key);
            for (int i = 0; i < _hashFunctionCount; i++)
            {
                int index = (int)(Math.Abs(hashes[i]) % _bitArray.Length);
                if (!_bitArray[index])
                    return false;
            }
            return true;
        }

        public byte[] Serialize()
        {
            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream);

            // Write metadata
            writer.Write(_bitArray.Length);
            writer.Write(_hashFunctionCount);
            writer.Write(_expectedFalsePositiveRate);

            // Write bit array
            var bytes = new byte[(_bitArray.Length + 7) / 8];
            _bitArray.CopyTo(bytes, 0);
            writer.Write(bytes.Length);
            writer.Write(bytes);

            return stream.ToArray();
        }

        public void Deserialize(byte[] data)
        {
            if (data == null)
                throw new ArgumentNullException(nameof(data));

            using var stream = new MemoryStream(data);
            using var reader = new BinaryReader(stream);

            // Read metadata
            int bitArrayLength = reader.ReadInt32();
            int hashFunctionCount = reader.ReadInt32();
            double falsePositiveRate = reader.ReadDouble();

            // Validate metadata
            if (bitArrayLength != _bitArray.Length)
                throw new InvalidOperationException("Bit array length mismatch");
            
            if (hashFunctionCount != _hashFunctionCount)
                throw new InvalidOperationException("Hash function count mismatch");

            // Read bit array
            int byteArrayLength = reader.ReadInt32();
            var bytes = reader.ReadBytes(byteArrayLength);
            
            var newBitArray = new BitArray(bytes) { Length = bitArrayLength };
            for (int i = 0; i < bitArrayLength; i++)
            {
                _bitArray[i] = newBitArray[i];
            }
        }

        private uint[] GetHashes(string key)
        {
            var hashes = new uint[_hashFunctionCount];
            var keyBytes = Encoding.UTF8.GetBytes(key);

            // Use different hash functions by varying the seed/salt
            for (int i = 0; i < _hashFunctionCount; i++)
            {
                hashes[i] = ComputeHash(keyBytes, (uint)i);
            }

            return hashes;
        }

        private static uint ComputeHash(byte[] data, uint seed)
        {
            // Simple FNV-1a hash with seed
            const uint fnvPrime = 16777619;
            uint hash = 2166136261u ^ seed;

            foreach (byte b in data)
            {
                hash ^= b;
                hash *= fnvPrime;
            }

            return hash;
        }

        public static BloomFilter Build(IEnumerable<string> keys, double falsePositiveRate = 0.01)
        {
            var keyList = keys.ToList();
            var filter = new BloomFilter(Math.Max(1, keyList.Count), falsePositiveRate);
            
            foreach (var key in keyList)
            {
                filter.Add(key);
            }

            return filter;
        }
    }
}
