using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Ingest;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;

namespace SimulationConsole
{
    public class StreamingLogger
    {
        #region Inner Types
        private record LogItem(DateTime timestamp, string level, string eventText);
        #endregion

        private readonly IKustoIngestClient _ingestClient;
        private readonly ConcurrentQueue<LogItem> _logQueue = new();
        private readonly Task _streamTask;
        private bool _isCompleting = false;

        public StreamingLogger(KustoConnectionStringBuilder connectionStringBuilder)
        {
            _ingestClient =
                KustoIngestFactory.CreateStreamingIngestClient(connectionStringBuilder);
            _streamTask = IngestLogsAsync();
        }

        public void Log(
            string level,
            string eventText)
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
            while (!_isCompleting)
            {
                await Task.Delay(TimeSpan.FromSeconds(5));
                if (_logQueue.Any())
                {
                    var builder = new StringBuilder();

                    while (_logQueue.TryDequeue(out var item))
                    {
                        builder.Append(item.timestamp);
                        builder.Append(", ");
                        builder.Append(Environment.MachineName);
                        builder.Append(", ");
                        builder.Append(item.level);
                        builder.Append(", \"");
                        builder.Append(item.eventText);
                        builder.Append('"');
                        builder.AppendLine();
                    }

                    using (var stream = new MemoryStream())
                    using (var writer = new StreamWriter(stream))
                    {
                        writer.Write(builder);
                        writer.Flush();
                        stream.Position = 0;
                        await _ingestClient.IngestFromStreamAsync(
                            stream,
                            new KustoIngestionProperties()
                            {
                                TableName = "Logs",
                                Format = DataSourceFormat.csv
                            });
                    }
                }
            }
        }
    }
}