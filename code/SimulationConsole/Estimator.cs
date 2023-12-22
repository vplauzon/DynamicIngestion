using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimulationConsole
{
    internal class Estimator
    {
        public TimeSpan DurationPerMb { get; private set; } = TimeSpan.FromSeconds(0.01);
    }
}