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
using Azure.Storage.Blobs.Models;

namespace SimulationConsole
{
    internal class Importer : IBatchIngestionQueue
    {
        #region Inner types
        private record IngestionResult(
            Guid operationId,
            DateTime lastUpdatedOn,
            TimeSpan duration,
            string state,
            string status,
            bool shouldRetry);
        #endregion

        private readonly ConcurrentQueue<IImmutableList<BlobItem>> _importerQueue = new();
        private readonly ICslAdminProvider _kustoProvider;
        private readonly string _database;
        private readonly Estimator _estimator;
        private readonly StreamingLogger _logger;
        private bool _isCompleting = false;

        #region Constructors
        private Importer(
            ICslAdminProvider kustoProvider,
            string database,
            Estimator estimator,
            StreamingLogger logger)
        {
            _kustoProvider = kustoProvider;
            _database = database;
            _estimator = estimator;
            _logger = logger;
        }

        public static Importer CreateImporter(
            KustoConnectionStringBuilder connectionStringBuilder,
            string database,
            Estimator estimator,
            StreamingLogger logger)
        {
            var kustoProvider = KustoClientFactory.CreateCslAdminProvider(
                connectionStringBuilder);

            return new Importer(kustoProvider, database, estimator, logger);
        }
        #endregion

        public async Task RunAsync()
        {
            var ingestionCapacity = await FetchIngestionCapacityAsync();
            var operationMap = new Dictionary<Guid, IImmutableList<BlobItem>>();

            while (_importerQueue.Any() || operationMap.Any() || !_isCompleting)
            {
                if (operationMap.Count < ingestionCapacity
                    && _importerQueue.TryDequeue(out var items))
                {
                    if (operationMap.Count + 1 == ingestionCapacity)
                    {   //  Clean operation map to confirm we're at capacity
                        await MonitorOperationsAsync(operationMap);
                        if (operationMap.Count + 1 == ingestionCapacity)
                        {   //  We are at capacity:  flip to backlogging mode
                            while (_importerQueue.TryPeek(out var peekItems))
                            {
                                var totalSize = items
                                    .Concat(peekItems)
                                    .Sum(i => i.size);
                                var estimatedIngestionTime = _estimator.EstimateTime(totalSize);

                                if (estimatedIngestionTime <= TimeSpan.FromMinutes(1))
                                {
                                    if (_importerQueue.TryDequeue(out peekItems))
                                    {
                                        items = items.AddRange(peekItems);
                                    }
                                    else
                                    {
                                        throw new NotSupportedException(
                                            "Inconsistant importer queue");
                                    }
                                }
                            }
                        }
                    }
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
                    .Select(row => new IngestionResult(
                        (Guid)row["OperationId"],
                        (DateTime)row["LastUpdatedOn"],
                        (TimeSpan)row["Duration"],
                        (string)row["State"],
                        (string)row["Status"],
                        (bool)row["ShouldRetry"]));
                var completedOperationIds = new List<Guid>();

                foreach (var result in results)
                {
                    switch (result.state)
                    {
                        case "InProgress":
                            break;
                        case "Completed":
                            completedOperationIds.Add(result.operationId);
                            CompleteBatch(result, operationMap);
                            break;
                        case "Failed":
                            throw new InvalidDataException($"Failed ingestion:  {result.status}");

                        default:
                            throw new NotImplementedException();
                    }
                }
            }
        }

        private void CompleteBatch(
            IngestionResult result,
            IDictionary<Guid, IImmutableList<BlobItem>> operationMap)
        {
            var items = operationMap[result.operationId];

            _logger.Log(
                LogLevel.Information,
                $"Ingest Op:  opid={result.operationId}, "
                + $"itemCount=\"{items.Count}\", "
                + $"status=\"{result.status}\", "
                + $"duration=\"{result.duration}\"");
            foreach (var item in items)
            {
                _logger.Log(
                    LogLevel.Information,
                    $"Ingest blob:  opid={result.operationId}, "
                    + $"itemId={item.ItemId}, "
                    + $"uri={item.uri}, "
                    + $"size={item.size}, "
                    + $"eventStart={item.eventStart}, "
                    + $"eventEnd={result.lastUpdatedOn}, "
                    + $"ingestionDuration={result.duration}");
            }
            _estimator.AddSizeDataPoint(
                items.Sum(i => i.size),
                result.duration);
            operationMap.Remove(result.operationId);
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
                _database,
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