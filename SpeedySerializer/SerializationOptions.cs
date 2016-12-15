using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NeoSmart.SpeedySerializer
{
    public class SerializationOptions
    {
        public string BaseDirectory { get; set; } = "";
        public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.Fastest;
        public SerializationMethod Engine = SerializationMethod.NewtonSoft;
    }
}
