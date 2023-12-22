﻿using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
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
        private record QueueItem(Uri uri, long size, DateTime eventStart);
        #endregion

        private readonly ConcurrentQueue<QueueItem> _queue = new();
        private readonly ICslAdminProvider _kustoProvider;
        private bool _isCompleting = false;

        #region Constructors
        private Aggregator(ICslAdminProvider kustoProvider)
        {
            _kustoProvider = kustoProvider;
        }

        public static async Task<Aggregator> CreateAggregatorAsync(Uri clusterUri)
        {
            var connectionStringBuilder = new KustoConnectionStringBuilder(clusterUri.ToString())
                .WithAadAzCliAuthentication();
            var kustoProvider = KustoClientFactory.CreateCslAdminProvider(
                connectionStringBuilder);

            return new Aggregator(kustoProvider);
        }
        #endregion

        void IIngestionQueue.PushUri(Uri uri, long size, DateTime eventStart)
        {
            _queue.Enqueue(new QueueItem(uri, size, eventStart));
        }

        public async Task RunAsync()
        {
            while (_isCompleting)
            {
                while (_queue.TryDequeue(out var item))
                {
                }
                await Task.Delay(TimeSpan.FromSeconds(1));
            }
        }

        public void Complete()
        {
            _isCompleting = true;
        }
    }
}
