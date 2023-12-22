﻿namespace SimulationConsole
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var runSettings = RunSettings.FromEnvironmentVariables();

            runSettings.WriteOutSettings();

            var aggregator = await Aggregator.CreateAggregatorAsync(runSettings.ClusterUri);
            var dataConnection = await DataConnection.CreateDataConnectionAsync(
                runSettings.SourceBlobPrefixUri,
                runSettings.SourceCount,
                aggregator);
            var aggregatorTask = aggregator.RunAsync();
            var dataConnectionTask = dataConnection.RunAsync(TimeSpan.FromMinutes(5));

            await dataConnectionTask;
            aggregator.Complete();
            await aggregatorTask;
        }
    }
}