using FASTER.core;
using System;
using System.Linq;

namespace MicrosoftFaster
{
    internal class FasterKey : IFasterEqualityComparer<FasterKey>
    {
        public byte[] Key { get; set; }
        public long GetHashCode64(ref FasterKey k)
        {
            Key = k.Key;
            var hash256 = Hash256(Key);
            long res = 0;
            foreach (var bt in hash256)
            {
                res = res * 31 * 31 * bt + 17;
            }

            return res;
        }

        public bool Equals(ref FasterKey k1, ref FasterKey k2)
        {
            return k1.Key.SequenceEqual(k2.Key);
        }

        private static byte[] Hash256(byte[] byteContents)
        {
            using var hash = new System.Security.Cryptography.SHA256CryptoServiceProvider();
            return hash.ComputeHash(byteContents);
        }
    }

    internal class FasterKeySerializer : BinaryObjectSerializer<FasterKey>
    {
        public override void Deserialize(out FasterKey obj)
        {
            obj = new FasterKey();

            var bytes = new byte[4];
            reader.Read(bytes, 0, 4);
            var size = BitConverter.ToInt32(bytes, 0);
            obj.Key = new byte[size];
            reader.Read(obj.Key, 0, size);
        }

        public override void Serialize(ref FasterKey obj)
        {
            var len = BitConverter.GetBytes(obj.Key.Length);
            writer.Write(len);
            writer.Write(obj.Key);
        }
    }

    internal class FasterValue
    {
        public byte[] Value;
    }

    internal class FasterValueSerializer : BinaryObjectSerializer<FasterValue>
    {
        public override void Deserialize(out FasterValue obj)
        {
            obj = new FasterValue();
            var bytes = new byte[4];
            reader.Read(bytes, 0, 4);
            var size = BitConverter.ToInt32(bytes, 0);
            obj.Value = reader.ReadBytes(size);
        }

        public override void Serialize(ref FasterValue obj)
        {
            var len = BitConverter.GetBytes(obj.Value.Length);
            writer.Write(len);
            writer.Write(obj.Value);
        }
    }
}
