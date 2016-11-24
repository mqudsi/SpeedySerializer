using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NeoSmart.SpeedySerializer
{
    class Lz4Object<T>
    {
        private byte[] _byteStream;

        public Lz4Object()
        {
        }

        public Lz4Object(T t)
        {
            Box(t);
        }

        //Generic boxing
        public void Box(T t)
        {
            var bytes = Encoding.UTF8.GetBytes(NetJSON.NetJSON.Serialize(t));
            _byteStream = Lz4Net.Lz4.CompressBytes(bytes);
        }

        public void Unbox(out T t)
        {
            var uncompressed = Lz4Net.Lz4.DecompressBytes(_byteStream);
            t = NetJSON.NetJSON.Deserialize<T>(Encoding.UTF8.GetString(uncompressed));
        }

        //String boxing
        public void Box(string t)
        {
            var bytes = Encoding.UTF8.GetBytes(t);
            _byteStream = Lz4Net.Lz4.CompressBytes(bytes);
        }

        public void Unbox(out string t)
        {
            var uncompressed = Lz4Net.Lz4.DecompressBytes(_byteStream);
            t = Encoding.UTF8.GetString(uncompressed);
        }
    }
}
