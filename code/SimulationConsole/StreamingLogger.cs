using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Ingest;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace SimulationConsole
{
    public class StreamingLogger
    {
        #region Inner Types
        private record LogItem(DateTime timestamp, LogLevel level, string eventText);
        #endregion

        private const int STREAMING_LIMIT = 4 * 1024 * 1024;

        private readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
        {
            WriteIndented = false
        };
        private readonly ConcurrentQueue<LogItem> _logQueue = new();
        private readonly KustoIngestionProperties _ingestionProperties;
        private readonly IKustoIngestClient _ingestClient;
        private readonly Task _streamTask;
        private bool _isCompleting = false;

        public StreamingLogger(
            KustoConnectionStringBuilder connectionStringBuilder,
            string database)
        {
            _ingestionProperties = new()
            {
                IgnoreFirstRecord = false,
                DatabaseName = database,
                TableName = "Logs",
                Format = DataSourceFormat.json
            };
            _ingestClient =
                KustoIngestFactory.CreateStreamingIngestClient(connectionStringBuilder);
            _streamTask = IngestLogsAsync();
        }

        public void Log(LogLevel level, string eventText)
        {
            _logQueue.Enqueue(new LogItem(DateTime.Now, level, eventText));
        }

        public async Task CompleteAsync()
        {
            _isCompleting = true;
            await _streamTask;
        }

        private async Task IngestLogsAsync()
        {
            while (!_isCompleting || _logQueue.Any())
            {
                var doContinue = true;

                await Task.Delay(TimeSpan.FromSeconds(1));
                while (doContinue && _logQueue.Any())
                {
                    using (var stream = new MemoryStream())
                    {
                        while (stream.Length < STREAMING_LIMIT
                            && _logQueue.TryDequeue(out var item))
                        {
                            var jsonObj = new
                            {
                                Timestamp = item.timestamp,
                                Source = Environment.MachineName,
                                Level = item.level.ToString(),
                                EventText = item.eventText
                            };
                            JsonSerializer.Serialize(stream, jsonObj, _jsonOptions);
                            stream.WriteByte((byte)'\n');
                        }
                        doContinue = stream.Length >= STREAMING_LIMIT;
                        stream.Position = 0;
                        await _ingestClient.IngestFromStreamAsync(stream, _ingestionProperties);
                    }
                }
            }
        }
    }
}