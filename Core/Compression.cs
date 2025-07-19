using System;
using System.IO;
using System.IO.Compression;

namespace LSMTree.Core
{
    public enum CompressionType : byte
    {
        None = 0,
        GZip = 1,
        LZ4 = 2,
        Snappy = 3
    }

    public interface ICompressor
    {
        CompressionType Type { get; }
        byte[] Compress(byte[] data);
        byte[] Decompress(byte[] data);
    }

    public class NoCompression : ICompressor
    {
        public CompressionType Type => CompressionType.None;

        public byte[] Compress(byte[] data)
        {
            return data;
        }

        public byte[] Decompress(byte[] data)
        {
            return data;
        }
    }

    public class GZipCompressor : ICompressor
    {
        public CompressionType Type => CompressionType.GZip;

        public byte[] Compress(byte[] data)
        {
            using var output = new MemoryStream();
            using var compressionStream = new GZipStream(output, CompressionLevel.Fastest);
            compressionStream.Write(data, 0, data.Length);
            compressionStream.Close();
            return output.ToArray();
        }

        public byte[] Decompress(byte[] data)
        {
            using var input = new MemoryStream(data);
            using var compressionStream = new GZipStream(input, CompressionMode.Decompress);
            using var output = new MemoryStream();
            compressionStream.CopyTo(output);
            return output.ToArray();
        }
    }

    public class DeflateCompressor : ICompressor
    {
        public CompressionType Type => CompressionType.LZ4;

        public byte[] Compress(byte[] data)
        {
            using var output = new MemoryStream();
            using var compressionStream = new DeflateStream(output, CompressionLevel.Fastest);
            compressionStream.Write(data, 0, data.Length);
            compressionStream.Close();
            return output.ToArray();
        }

        public byte[] Decompress(byte[] data)
        {
            using var input = new MemoryStream(data);
            using var compressionStream = new DeflateStream(input, CompressionMode.Decompress);
            using var output = new MemoryStream();
            compressionStream.CopyTo(output);
            return output.ToArray();
        }
    }

    public static class CompressionFactory
    {
        public static ICompressor Create(CompressionType type)
        {
            return type switch
            {
                CompressionType.None => new NoCompression(),
                CompressionType.GZip => new GZipCompressor(),
                CompressionType.LZ4 => new DeflateCompressor(),
                _ => throw new ArgumentException($"Unsupported compression type: {type}")
            };
        }
    }
}
