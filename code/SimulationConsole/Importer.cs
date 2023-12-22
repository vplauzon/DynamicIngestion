using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Kusto.Cloud.Platform.Data;
using System.Collections.Immutable;
using System.Collections.Concurrent;

namespace SimulationConsole
{
    internal class Importer : IBatchIngestionQueue
    {
        private readonly ConcurrentQueue<IImmutableList<BlobItem>> _importerQueue = new();
        private readonly ICslAdminProvider _kustoProvider;
        private readonly Estimator _estimator;
        private readonly StreamingLogger _logger;
        private bool _isCompleting = false;

        #region Constructors
        private Importer(
            ICslAdminProvider kustoProvider,
            Estimator estimator,
            StreamingLogger logger)
        {
            _kustoProvider = kustoProvider;
            _estimator = estimator;
            _logger = logger;
        }

        public static Importer CreateImporter(
            KustoConnectionStringBuilder connectionStringBuilder,
            Estimator estimator,
            StreamingLogger logger)
        {
            var kustoProvider = KustoClientFactory.CreateCslAdminProvider(
                connectionStringBuilder);

            return new Importer(kustoProvider, estimator, logger);
        }
        #endregion

        public async Task RunAsync()
        {
            var ingestionCapacity = await FetchIngestionCapacityAsync();

            while (!_isCompleting)
            {
                if (_importerQueue.TryDequeue(out var items))
                {
                    await PushIngestionAsync(items);
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

        void IBatchIngestionQueue.Push(IEnumerable<BlobItem> items)
        {
            _importerQueue.Enqueue(items.ToImmutableArray());
        }

        private async Task PushIngestionAsync(IImmutableList<BlobItem> items)
        {
            var uriList = string.Join(
                "," + Environment.NewLine,
                items.Select(i => $"@'{i.uri}'"));
            var commandText = $@"
.ingest async into table IngestTest
(
    {uriList}
)
with (format='csv')
";
            var reader = await _kustoProvider.ExecuteControlCommandAsync(
                string.Empty,
                commandText,
                null);
            var table = reader.ToDataSet().Tables[0];
            var operationId = (Guid)table.Rows[0][0];

            throw new NotImplementedException();
        }

        private async Task<int> FetchIngestionCapacityAsync()
        {
            var reader = await _kustoProvider.ExecuteControlCommandAsync(
                string.Empty,
                @".show capacity ingestions
| project Total",
                null);
            var table = reader.ToDataSet().Tables[0];
            var ingestionCapacity = (long)table.Rows[0][0];

            return (int)ingestionCapacity;
        }
    }
}