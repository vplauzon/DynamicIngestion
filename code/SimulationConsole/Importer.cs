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
using System.Data;

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
            var operationMap = new Dictionary<Guid, IImmutableList<BlobItem>>();

            while (!_isCompleting)
            {
                if (operationMap.Count < ingestionCapacity
                    && _importerQueue.TryDequeue(out var items))
                {
                    var operationId = await PushIngestionAsync(items);

                    operationMap.Add(operationId, items);
                }
                else
                {
                    await MonitorOperationsAsync(operationMap);
                    _logger.Log(
                        LogLevel.Information,
                        $"Ingest Capacity Used Length={operationMap.Count}");
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

        private async Task MonitorOperationsAsync(
            IDictionary<Guid, IImmutableList<BlobItem>> operationMap)
        {
            if (operationMap.Any())
            {
                var operationIdList = string.Join(
                    ", ",
                    operationMap.Keys.Select(id => id.ToString()));
                var commandText = $@"
.show operations
(
    {operationIdList}
)
";
                var reader = await _kustoProvider.ExecuteControlCommandAsync(
                    string.Empty,
                    commandText,
                    null);
                var table = reader.ToDataSet().Tables[0];
                var results = table.Rows
                    .Cast<DataRow>()
                    .Select(row => new
                    {
                        OperationId = (Guid)row["OperationId"],
                        Duration = (TimeSpan)row["Duration"],
                        State = (string)row["State"],
                        Status = (string)row["Status"],
                        ShouldRetry = (bool)row["ShouldRetry"]
                    });
                var completedOperationIds = new List<Guid>();

                foreach (var result in results)
                {
                    switch (result.State)
                    {
                        case "InProgress":
                            break;
                        case "Completed":
                            completedOperationIds.Add(result.OperationId);
                            _logger.Log(
                                LogLevel.Information,
                                $"Ingest:  opid={result.OperationId}, "
                                + $"status=\"{result.Status}\""
                                + $"duration=\"{result.Duration}\"");
                            _estimator.AddSizeDataPoint(
                                operationMap[result.OperationId].Sum(i => i.size),
                                result.Duration);
                            operationMap.Remove(result.OperationId);
                            break;
                        case "Failed":
                            throw new InvalidDataException($"Failed ingestion:  {result.Status}");

                        default:
                            throw new NotImplementedException();
                    }
                }
            }
        }

        private async Task<Guid> PushIngestionAsync(IImmutableList<BlobItem> items)
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

            return operationId;
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