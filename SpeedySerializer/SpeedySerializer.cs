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
        public static readonly SerializationOptions Defaults = new SerializationOptions();
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

        private static string FileName(string baseDirectory, string naymspace, string key)
        {
            var namespaceBytes = Encoding.UTF8.GetBytes(naymspace);
            var namespaceHash = MetroHash128.Hash(0, namespaceBytes, 0, namespaceBytes.Length);

            var keyBytes = Encoding.UTF8.GetBytes(key);
            var keyHash = MetroHash128.Hash(BitConverter.ToUInt64(namespaceHash, 0), keyBytes, 0, keyBytes.Length);
            
            var base64 = Convert.ToBase64String(keyHash);
            base64 = base64.TrimEnd('=') + ".ss";

            if (!string.IsNullOrWhiteSpace(baseDirectory))
            {
                return Path.Combine(baseDirectory, base64);
            }
            return base64;
        }

        public static void Serialize<T>(string naymspace, string key, T o, SerializationOptions options = null)
        {
            if (options == null)
            {
                options = Defaults;
            }

            using (var stream = File.Open(FileName(options.BaseDirectory, naymspace, key), FileMode.Create, FileAccess.ReadWrite, FileShare.None))
            using (var cstream = CompressionStream(options.CompressionLevel, stream))
            using (var writer = new StreamWriter(cstream, Encoding.UTF8, 4096, true))
            {
                switch (options.Engine)
                {
                    case SerializationMethod.NetJson:
                        NetJSON.NetJSON.Serialize(o, writer);
                        break;
                    case SerializationMethod.Jil:
                        Jil.JSON.Serialize(o, writer);
                        break;
                    case SerializationMethod.Bson:
                    case SerializationMethod.NewtonSoft:
                        var serializer = Newtonsoft.Json.JsonSerializer.Create();
                        var jWriter = options.Engine == SerializationMethod.Bson
                            ? new Newtonsoft.Json.Bson.BsonWriter(cstream)
                            : (Newtonsoft.Json.JsonWriter) new Newtonsoft.Json.JsonTextWriter(writer);
                        serializer.TypeNameHandling = Newtonsoft.Json.TypeNameHandling.Auto;
                        if (options.CompressionLevel == CompressionLevel.None)
                        {
                            serializer.Formatting = Newtonsoft.Json.Formatting.Indented;
                        }
                        serializer.ContractResolver = new PrivateSetterResolver();
                        serializer.Serialize(jWriter, o);
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }
        }

        public static void Serialize<T>(string key, T o, SerializationOptions options = null)
        {
            Serialize(string.Empty, key, o, options);
        }

        public static T Deserialize<T>(string key, SerializationOptions options = null)
        {
            return Deserialize<T>(string.Empty, key, options);
        }

        public static T Deserialize<T>(string naymspace, string key, SerializationOptions options = null)
        {
            options = options ?? Defaults;

            var filename = FileName(options.BaseDirectory, naymspace, key);
            using (var stream = File.OpenRead(filename))
            using (var cstream = DecompressionStream(options.CompressionLevel, stream))
            using (var reader = new StreamReader(cstream, Encoding.UTF8, false, 4096, true))
            {
                switch (options.Engine)
                {
                    case SerializationMethod.NetJson:
                        return NetJSON.NetJSON.Deserialize<T>(reader);
                    case SerializationMethod.Jil:
                        return Jil.JSON.Deserialize<T>(reader);
                    case SerializationMethod.NewtonSoft:
                        using (var jReader = options.Engine == SerializationMethod.Bson ? 
                            new Newtonsoft.Json.Bson.BsonReader(cstream) : 
                            (Newtonsoft.Json.JsonReader) new Newtonsoft.Json.JsonTextReader(reader))
                        {
                            jReader.CloseInput = false;
                            var deserializer = Newtonsoft.Json.JsonSerializer.Create();
                            deserializer.TypeNameHandling = Newtonsoft.Json.TypeNameHandling.Auto;
                            deserializer.ContractResolver = new PrivateSetterResolver();
                            var deserialized = deserializer.Deserialize<T>(jReader);
                            return deserialized;
                        }
                    default:
                        throw new NotImplementedException();
                }
            }
        }

        public static bool TryDeserialize<T>(string naymspace, string key, ref T o, SerializationOptions options = null)
        {
            options = options ?? Defaults;

            try
            {
                o = Deserialize<T>(naymspace, key, options);
                return true;
            }
            catch (FileNotFoundException)
            {
                return false;
            }
            catch (Exception ex)
            {
                //delete the file because it seems to be corrupted, maybe?
                var filename = FileName(options.BaseDirectory, naymspace, key);
                if (File.Exists(filename))
                {
                    File.Delete(filename);
                }
                throw ex;
                return false;
            }
        }

        public static bool TryDeserialize<T>(string key, ref T o, SerializationOptions options = null)
        {
            return TryDeserialize(string.Empty, key, ref o, options);
        }

        public static bool Backup(string naymspace, string key, SerializationOptions options = null)
        {
            var filename = FileName(options?.BaseDirectory ?? Defaults.BaseDirectory, naymspace, key);
            if (File.Exists(filename))
            {
                File.Copy(filename, $"{filename}.bak", true);
                return true;
            }
            return false;
        }

        public static bool Backup(string key, SerializationOptions options = null)
        {
            return Backup(string.Empty, key, options);
        }
    }
}
