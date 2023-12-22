namespace SimulationConsole
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var runSettings = RunSettings.FromEnvironmentVariables();

            runSettings.WriteOutSettings();

            var estimator = new Estimator();
            var importer = Importer.CreateImporter(
                runSettings.ClusterUri,
                estimator);
            var aggregator = Aggregator.CreateAggregator(estimator);
            var dataConnection = await DataConnection.CreateDataConnectionAsync(
                runSettings.SourceBlobPrefixUri,
                runSettings.SourceCount,
                aggregator);
            var importerTask = importer.RunAsync();
            var aggregatorTask = aggregator.RunAsync();
            var dataConnectionTask = dataConnection.RunAsync(TimeSpan.FromMinutes(5));

            await dataConnectionTask;
            importer.Complete();
            aggregator.Complete();
            await aggregatorTask;
            await importerTask;
        }
    }
}