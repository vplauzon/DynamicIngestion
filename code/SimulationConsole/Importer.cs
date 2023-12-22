using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Kusto.Cloud.Platform.Data;
using Azure.Identity;

namespace SimulationConsole
{
    internal class Importer
    {
        private readonly ICslAdminProvider _kustoProvider;
        private readonly Estimator _estimator;
        private bool _isCompleting = false;

        #region Constructors
        private Importer(ICslAdminProvider kustoProvider, Estimator estimator)
        {
            _kustoProvider = kustoProvider;
            _estimator = estimator;
        }

        public static Importer CreateImporter(
            KustoConnectionStringBuilder connectionStringBuilder,
            Estimator estimator)
        {
            var kustoProvider = KustoClientFactory.CreateCslAdminProvider(
                connectionStringBuilder);

            return new Importer(kustoProvider, estimator);
        }
        #endregion

        public async Task RunAsync()
        {
            var ingestionCapacity = await FetchIngestionCapacityAsync();

            while (_isCompleting)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
            }
        }

        public void Complete()
        {
            _isCompleting = true;
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