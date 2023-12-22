using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimulationConsole
{
    internal interface IBatchIngestionQueue
    {
        void Push(IEnumerable<BlobItem> items);
    }
}