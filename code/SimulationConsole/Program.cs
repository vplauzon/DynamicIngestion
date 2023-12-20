namespace SimulationConsole
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var runSettings = RunSettings.FromEnvironmentVariables();

            runSettings.WriteOutSettings();
        }
    }
}