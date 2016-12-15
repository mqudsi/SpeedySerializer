using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NeoSmart.SpeedySerializer
{
    public enum CompressionLevel
    {
        None,
        Fastest,
        Balanced,
        Smallest,
    }

    public enum SerializationMethod
    {
        NetJson,
        NewtonSoft,
        Bson,
        BinaryFormatter,
        Jil,
    }
}
