using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimulationConsole
{
    internal partial class Aggregator : IIngestionQueue
    {
        private readonly ConcurrentQueue<BlobItem> _queue = new();
        private readonly TimeSpan _sloTime;
        private readonly Estimator _estimator;
        private readonly IBatchIngestionQueue _batchIngestionQueue;
        private readonly StreamingLogger _logger;
        private bool _isCompleting = false;

        #region Constructors
        private Aggregator(
            TimeSpan sloTime,
            Estimator estimator,
            IBatchIngestionQueue batchIngestionQueue,
            StreamingLogger logger)
        {
            _sloTime = sloTime;
            _estimator = estimator;
            _batchIngestionQueue = batchIngestionQueue;
            _logger = logger;
        }

        public static Aggregator CreateAggregator(
            TimeSpan sloTime,
            Estimator estimator,
            IBatchIngestionQueue batchIngestionQueue,
            StreamingLogger logger)
        {
            return new Aggregator(sloTime, estimator, batchIngestionQueue, logger);
        }
        #endregion

        void IIngestionQueue.Push(BlobItem item)
        {
            _queue.Enqueue(item);
        }

        public async Task RunAsync()
        {
            var currentBatch = new List<BlobItem>();

            while (!_isCompleting)
            {
                if (currentBatch.Any())
                {
                    var now = DateTime.Now;
                    var maxAge = currentBatch.Max(i => i.eventStart.Subtract(now));
                    var totalSize = currentBatch.Sum(i => i.size);
                    var estimatedIngestionTime = _estimator.EstimateTime(totalSize);

                    if (estimatedIngestionTime + maxAge > _sloTime)
                    {
                    }
                }
                if (_queue.TryDequeue(out var item))
                {
                    currentBatch.Add(item);
                }
                else
                {
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            }
        }

        public void Complete()
        {
            _isCompleting = true;
        }
    }
}
