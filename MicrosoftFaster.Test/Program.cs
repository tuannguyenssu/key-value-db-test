using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace MicrosoftFaster.Test
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var db = new FasterDb("D:\\LOG\\FasterTest");
            var tasks = new List<Task>();
            for (int i = 0; i < 1000000; i++)
            {
              //  var task = db.PutAsync($"{i}", Encoding.UTF8.GetBytes($"{i}"));
                await db.PutAsync($"{i}", Encoding.UTF8.GetBytes($"{i}"));
                var test = await db.GetAsync($"{i}");
                Console.WriteLine(Encoding.UTF8.GetString(test!));

                //tasks.Add(task);
            }

            //await Task.WhenAll(tasks);

            await db.SaveCheckpointAsync();
            Console.WriteLine("Finished!");
            Console.ReadKey();
        }
    }
}
