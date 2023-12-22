using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimulationConsole
{
    internal class Estimator
    {
        private const long MB = 1024 * 1024;

        public TimeSpan DurationPerMb { get; private set; } = TimeSpan.FromSeconds(1);

        public TimeSpan EstimateTime(long size)
        {
            return DurationPerMb.Multiply((double)size / MB);
        }

        public void AddSizeDataPoint(long size, TimeSpan duration)
        {
        }
    }
}