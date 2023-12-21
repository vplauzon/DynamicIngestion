using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimulationConsole
{
    internal class Aggregator : IIngestionQueue
    {
        void IIngestionQueue.PushUri(Uri uri)
        {
        }
    }
}
