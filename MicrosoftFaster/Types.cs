using FASTER.core;
using System;
using System.Linq;

namespace MicrosoftFaster
{
    internal class FasterData : IFasterEqualityComparer<FasterData>
    {
        public byte[] Data { get; set; }
        public long GetHashCode64(ref FasterData data)
        {
            Data = data.Data;
            var hash256 = Hash256(Data);
            long res = 0;
            foreach (var bt in hash256)
            {
                res = res * 31 * 31 * bt + 17;
            }
            return res;
        }

        public bool Equals(ref FasterData d1, ref FasterData d2)
        {
            return d1.Data.SequenceEqual(d2.Data);
        }

        private static byte[] Hash256(byte[] byteContents)
        {
            using var hash = new System.Security.Cryptography.SHA256CryptoServiceProvider();
            return hash.ComputeHash(byteContents);
        }
    }

    internal class FasterDataSerializer : BinaryObjectSerializer<FasterData>
    {
        public override void Deserialize(out FasterData data)
        {
            data = new FasterData();
            var size = BitConverter.ToInt32(reader.ReadBytes(sizeof(int)), 0);
            data.Data = reader.ReadBytes(size);
        }

        public override void Serialize(ref FasterData data)
        {
            var length = BitConverter.GetBytes(data.Data.Length);
            writer.Write(length);
            writer.Write(data.Data);
        }
    }
}
