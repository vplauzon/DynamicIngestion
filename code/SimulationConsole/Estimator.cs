using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimulationConsole
{
    internal class Estimator
    {
        #region Inner Types
        private record DurationPoint(DateTime time, long size, TimeSpan duration);
        #endregion

        private const long MB = 1024 * 1024;
        private static readonly TimeSpan TIME_HORIZON = TimeSpan.FromMinutes(5);

        private readonly List<DurationPoint> _durationPoints = new List<DurationPoint>();

        public TimeSpan DurationPerMb { get; private set; } = TimeSpan.FromSeconds(1);

        public TimeSpan EstimateTime(long size)
        {
            return DurationPerMb.Multiply((double)size / MB);
        }

        /// <summary>This method is assumed to be single-thread.</summary>
        /// <param name="size"></param>
        /// <param name="duration"></param>
        public void AddSizeDataPoint(long size, TimeSpan duration)
        {
            _durationPoints.Add(new DurationPoint(DateTime.Now, size, duration));
            CleanSizePoints();
            DurationPerMb = ComputeWeightedAverageTime();
        }

        private TimeSpan ComputeWeightedAverageTime()
        {
            var now = DateTime.Now;
            var weightedSum = TimeSpan.Zero;
            var weightSum = (double)0;

            foreach (var point in _durationPoints)
            {
                var weight = ComputeWeight(now, point.time);

                weightedSum += weight * (point.duration * ((double)MB / point.size));
                weightSum += weight;
            }

            return weightedSum / weightSum;
        }

        private double ComputeWeight(DateTime now, DateTime time)
        {   //  Linear interpolation with 1 for now and 0 at now-time-horizon
            //  W = 1 - (now-time)/(Time Horizon)
            var weight = 1 - now.Subtract(time).TotalSeconds / TIME_HORIZON.TotalSeconds;

            return weight;
        }

        private void CleanSizePoints()
        {
            var windowStart = DateTime.Now.Subtract(TIME_HORIZON);
            var startIndex = _durationPoints.FindIndex(p => p.time >= windowStart);

            if (startIndex > 0)
            {
                _durationPoints.RemoveRange(0, startIndex);
            }
        }
    }
}