namespace SimulationConsole
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var runSettings = RunSettings.FromEnvironmentVariables();

            runSettings.WriteOutSettings();

            var dataConnection = DataConnection.CreateDataConnectionAsync(
                runSettings.SourceBlobPrefixUri,
                runSettings.SourceCount);
        }
    }
}