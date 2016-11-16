using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MetroHash;
using System.IO;

namespace NeoSmart.SpeedySerializer
{
    public static class SpeedySerializer
    {
        public static string BaseDirectory = String.Empty;
        public static CompressionLevel CompressionLevel { get; set; } = CompressionLevel.Fastest;
        private static Stream CompressionStream(CompressionLevel level, Stream stream)
        {
            switch (level)
            {
                case CompressionLevel.None:
                    return stream;
                case CompressionLevel.Fastest:
                    return new Lz4Net.Lz4CompressionStream(stream);
                    //return new Snappy.SnappyStream(stream, System.IO.Compression.CompressionMode.Compress, true);
                case CompressionLevel.Balanced:
                    return new System.IO.Compression.DeflateStream(stream, System.IO.Compression.CompressionMode.Compress);
                    //return new Ionic.Zlib.ParallelDeflateOutputStream(stream);
                case CompressionLevel.Smallest:
                    return new ManagedXZ.XZCompressStream(stream);
                default:
                    throw new ArgumentException("Unsupported CompressionLevel!");
            }
        }

        private static Stream DecompressionStream(CompressionLevel level, Stream stream)
        {
            switch (level)
            {
                case CompressionLevel.None:
                    return stream;
                case CompressionLevel.Fastest:
                    return new Lz4Net.Lz4DecompressionStream(stream);
                    //return new Snappy.SnappyStream(stream, System.IO.Compression.CompressionMode.Decompress, true);
                case CompressionLevel.Balanced:
                    return new System.IO.Compression.DeflateStream(stream, System.IO.Compression.CompressionMode.Decompress);
                case CompressionLevel.Smallest:
                    return new ManagedXZ.XZDecompressStream(stream);
                default:
                    throw new ArgumentException("Unsupported CompressionLevel!");
            }
        }

        public static void Serialize<T>(string naymspace, string key, T o)
        {
            var namespaceBytes = Encoding.UTF8.GetBytes(naymspace);
            var namespaceHash = MetroHash128.Hash(0, namespaceBytes, 0, namespaceBytes.Length);

            var keyBytes = Encoding.UTF8.GetBytes(key);
            var keyHash = MetroHash128.Hash(BitConverter.ToUInt64(namespaceHash, 0), keyBytes, 0, keyBytes.Length);

            using (var stream = File.Open(Convert.ToBase64String(keyHash), FileMode.Create, FileAccess.ReadWrite, FileShare.None))
            using (var cstream = CompressionStream(CompressionLevel, stream))
            using (var writer = new StreamWriter(cstream, Encoding.UTF8, 4096, true))
            {
                NetJSON.NetJSON.Serialize<T>(o, writer);
            }
        }

        public static void Serialize<T>(string key, T o)
        {
            Serialize(String.Empty, key, o);
        }

        public static T Deserialize<T>(string key)
        {
            return Deserialize<T>(String.Empty, key);
        }

        public static T Deserialize<T>(string naymspace, string key)
        {
            var namespaceBytes = Encoding.UTF8.GetBytes(naymspace);
            var namespaceHash = MetroHash128.Hash(0, namespaceBytes, 0, namespaceBytes.Length);

            var keyBytes = Encoding.UTF8.GetBytes(key);
            var keyHash = MetroHash128.Hash(BitConverter.ToUInt64(namespaceHash, 0), keyBytes, 0, keyBytes.Length);

            using (var stream = File.OpenRead(Convert.ToBase64String(keyHash)))
            using (var cstream = DecompressionStream(CompressionLevel, stream))
            using (var reader = new StreamReader(cstream, Encoding.UTF8, false, 4096, true))
            {
                return NetJSON.NetJSON.Deserialize<T>(reader);
            }
        }

        public static bool TryDeserialize<T>(string naymspace, string key, ref T o)
        {
            try
            {
                o = Deserialize<T>(naymspace, key);
                return true;
            }
            catch
            {
                return false;
            }
        }

        public static bool TryDeserialize<T>(string key, ref T o)
        {
            return TryDeserialize(String.Empty, key, ref o);
        }
    }
}
