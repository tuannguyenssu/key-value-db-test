using System;
using System.Diagnostics;
using RocksDbSharp;

namespace RocksDb.Test
{
    class Program
    {
        static void Main(string[] args)
        {
            var options = new DbOptions().SetCreateIfMissing();
            using var db = RocksDbSharp.RocksDb.Open(options, "D:\\LOG\\RocksDbTest");
            var watch = new Stopwatch();
            watch.Start();
            for (int i = 0; i < 10000000; i++)
            {
                db.Put($"{i}", $"{i}");
                var test = db.Get($"{i}");
                //Console.WriteLine(test);
            }

            watch.Stop();

            Console.WriteLine(watch.ElapsedMilliseconds / 1000);

            Console.ReadKey();
        }
    }
}
