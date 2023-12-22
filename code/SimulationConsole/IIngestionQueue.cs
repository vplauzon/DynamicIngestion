using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimulationConsole
{
    internal interface IIngestionQueue
    {
        void PushUri(Uri uri, long size, DateTime eventStart);
    }
}