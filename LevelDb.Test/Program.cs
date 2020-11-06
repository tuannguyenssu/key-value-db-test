using System;
using System.Diagnostics;
using LevelDB;

namespace LevelDb.Test
{
    class Program
    {
        static void Main(string[] args)
        {
            var levelDbOptions = new Options()
            {
                CreateIfMissing = true,
                //Cache = new Cache(1024 * 1024 * 1024)
            };
            var levelDb = new DB(levelDbOptions, "D:\\LOG\\LevelDbTest");
            var watch = new Stopwatch();
            watch.Start();
            for (int i = 0; i < 10000000; i++)
            {
                //levelDb.Put($"{i}", $"{i}");
                var test = levelDb.Get($"{i}");
                Console.WriteLine(test);
            }

            watch.Stop();

            Console.WriteLine(watch.ElapsedMilliseconds / 1000);

            Console.ReadKey();
        }
    }
}
