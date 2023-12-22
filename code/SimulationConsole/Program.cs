﻿using Azure.Identity;
using Kusto.Data;

namespace SimulationConsole
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var runSettings = RunSettings.FromEnvironmentVariables();

            runSettings.WriteOutSettings();

            var connectionStringBuilder = new KustoConnectionStringBuilder(
                runSettings.ClusterUri.ToString())
                .WithAadAzureTokenCredentialsAuthentication(new DefaultAzureCredential());
            var logger = new StreamingLogger(connectionStringBuilder);
            var estimator = new Estimator();
            var importer = Importer.CreateImporter(
                connectionStringBuilder,
                estimator,
                logger);
            var aggregator = Aggregator.CreateAggregator(
                runSettings.SloTime,
                estimator,
                importer,
                logger);
            var dataConnection = await DataConnection.CreateDataConnectionAsync(
                runSettings.SourceBlobPrefixUri,
                runSettings.SourceCount,
                aggregator,
                logger);
            var importerTask = importer.RunAsync();
            var aggregatorTask = aggregator.RunAsync();
            var dataConnectionTask = dataConnection.RunAsync(TimeSpan.FromMinutes(5));

            await dataConnectionTask;
            importer.Complete();
            aggregator.Complete();
            await aggregatorTask;
            await importerTask;
            await logger.CompleteAsync();
        }
    }
}