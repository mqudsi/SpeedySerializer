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
        public static SerializationMethod Backend = SerializationMethod.NewtonSoft;
        public static string BaseDirectory = "";
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

        private static string FileName(string naymspace, string key)
        {
            var namespaceBytes = Encoding.UTF8.GetBytes(naymspace);
            var namespaceHash = MetroHash128.Hash(0, namespaceBytes, 0, namespaceBytes.Length);

            var keyBytes = Encoding.UTF8.GetBytes(key);
            var keyHash = MetroHash128.Hash(BitConverter.ToUInt64(namespaceHash, 0), keyBytes, 0, keyBytes.Length);
            
            var base64 = Convert.ToBase64String(keyHash);
            base64 = base64.TrimEnd('=') + ".ss";

            if (!string.IsNullOrWhiteSpace(BaseDirectory))
            {
                return Path.Combine(BaseDirectory, base64);
            }
            return base64;
        }

        public static void Serialize<T>(string naymspace, string key, T o)
        {
            using (var stream = File.Open(FileName(naymspace, key), FileMode.Create, FileAccess.ReadWrite, FileShare.None))
            using (var cstream = CompressionStream(CompressionLevel, stream))
            using (var writer = new StreamWriter(cstream, Encoding.UTF8, 4096, true))
            {
                if (Backend == SerializationMethod.NetJson)
                {
                    NetJSON.NetJSON.Serialize(o, writer);
                }
                else if (Backend == SerializationMethod.Jil)
                {
                    Jil.JSON.Serialize(o, writer);
                }
                else if (Backend == SerializationMethod.NewtonSoft)
                {
                    var serializer = Newtonsoft.Json.JsonSerializer.Create();
                    serializer.TypeNameHandling = Newtonsoft.Json.TypeNameHandling.Auto;
                    serializer.Formatting = Newtonsoft.Json.Formatting.Indented;
                    serializer.ContractResolver = new PrivateSetterResolver();
                    serializer.Serialize(writer, o);
                }
                else
                {
                    throw new NotImplementedException();
                }
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
            var filename = FileName(naymspace, key);
            using (var stream = File.OpenRead(filename))
            using (var cstream = DecompressionStream(CompressionLevel, stream))
            using (var reader = new StreamReader(cstream, Encoding.UTF8, false, 4096, true))
            {
                if (Backend == SerializationMethod.NetJson)
                {
                    return NetJSON.NetJSON.Deserialize<T>(reader);
                }
                else if (Backend == SerializationMethod.Jil)
                {
                    return Jil.JSON.Deserialize<T>(reader);
                }
                else if (Backend == SerializationMethod.NewtonSoft)
                {
                    using (var jReader = new Newtonsoft.Json.JsonTextReader(reader))
                    {
                        jReader.CloseInput = false;
                        var deserializer = Newtonsoft.Json.JsonSerializer.Create();
                        deserializer.TypeNameHandling = Newtonsoft.Json.TypeNameHandling.Auto;
                        deserializer.ContractResolver = new PrivateSetterResolver();
                        var deserialized = deserializer.Deserialize<T>(jReader);
                        return deserialized;
                    }
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
        }

        public static bool TryDeserialize<T>(string naymspace, string key, ref T o)
        {
            try
            {
                o = Deserialize<T>(naymspace, key);
                return true;
            }
            catch (FileNotFoundException)
            {
                return false;
            }
            catch (Exception ex)
            {
                //delete the file because it seems to be corrupted, maybe?
                var filename = FileName(naymspace, key);
                if (File.Exists(filename))
                {
                    File.Delete(filename);
                }
                throw;
                return false;
            }
        }

        public static bool TryDeserialize<T>(string key, ref T o)
        {
            return TryDeserialize(String.Empty, key, ref o);
        }
    }
}
