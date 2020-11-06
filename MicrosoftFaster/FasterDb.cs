using System;
using FASTER.core;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace MicrosoftFaster
{
    public class FasterDb : IDisposable
    {
        private readonly FasterKV<FasterData, FasterData> _fasterKv;
        private readonly IDevice _log;
        private readonly IDevice _objLog;
        private readonly ClientSession<FasterData, FasterData, FasterData, FasterData, Empty, IFunctions<FasterData, FasterData, FasterData, FasterData, Empty>> _session;

        public FasterDb(string fileName)
        {
            const long logSize = 1L << 20;
            _log = Devices.CreateLogDevice(@$"{fileName}\{nameof(FasterDb)}.log");
            _objLog = Devices.CreateLogDevice(@$"{fileName}\{nameof(FasterDb)}-obj.log");
            _fasterKv = new FasterKV
                <FasterData, FasterData>(
                    logSize,
                    new LogSettings
                    {
                        LogDevice = _log,
                        ObjectLogDevice = _objLog,
                        MutableFraction = 0.3,
                        PageSizeBits = 15,
                        MemorySizeBits = 20
                    },
                    new CheckpointSettings
                    {
                        CheckpointDir = $"{fileName}/Checkpoints"
                    },
                    new SerializerSettings<FasterData, FasterData>
                    {
                        keySerializer = () => new FasterDataSerializer(),
                        valueSerializer = () => new FasterDataSerializer()
                    }
                );

            if (Directory.Exists($"{fileName}/Checkpoints"))
            {
                _fasterKv.Recover();
            }

            _session = _fasterKv.NewSession(new SimpleFunctions<FasterData, FasterData>());
        }

        public void Put(string key, byte[] data)
        {
            _session.Upsert(new FasterData()
            {
                Data = Encoding.UTF8.GetBytes(key)
            }, new FasterData()
            {
                Data = data
            });
        }

        public byte[] Get(string key)
        {
            var storeValue = _session.Read(new FasterData()
            {
                Data = Encoding.UTF8.GetBytes(key)
            });

            return storeValue.Item2.Data;
        }

        public async Task PutAsync(string key, byte[] data)
        {
            var result = await _session.RMWAsync(new FasterData()
            {
                Data = Encoding.UTF8.GetBytes(key)
            }, new FasterData()
            {
                Data = data
            });

            await result.CompleteAsync();
        }

        public async ValueTask<byte[]> GetAsync(string key)
        {
            var result = await _session.ReadAsync(new FasterData()
            {
                Data = Encoding.UTF8.GetBytes(key)
            });

            var (status, data) = result.Complete();

            return status == Status.OK ? data.Data : default;
        }



        public async ValueTask<bool> SaveCheckpointAsync()
        {
            //(bool isSuccess, _) = await _fasterKv.TakeFullCheckpointAsync(CheckpointType.Snapshot);
            (bool isSuccess, _) = await _fasterKv.TakeHybridLogCheckpointAsync(CheckpointType.Snapshot);
            await _fasterKv.CompleteCheckpointAsync();
            return isSuccess;
        }

        public void Dispose()
        {
            _fasterKv.Dispose();
            _log.Dispose();
            _objLog.Dispose();
        }
    }
}
