using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimulationConsole
{
    internal class Aggregator : IIngestionQueue
    {
        #region Inner Types
        private record QueueItem(Uri uri, DateTime eventStart);
        #endregion

        private readonly ConcurrentQueue<QueueItem> _queue = new();

        void IIngestionQueue.PushUri(Uri uri, DateTime eventStart)
        {
            _queue.Enqueue(new QueueItem(uri, eventStart));
        }
    }
}
