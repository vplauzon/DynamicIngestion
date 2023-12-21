using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;

namespace SimulationConsole
{
    internal class DataConnection
    {
        //private readonly Uri _sourceBlobPrefixUri;

        #region Constructors
        private DataConnection()
        {
        }

        public static async Task<DataConnection> CreateDataConnectionAsync(
            Uri sourceBlobPrefixUri,
            int? sourceCount)
        {
            var prefixClient = new BlockBlobClient(sourceBlobPrefixUri);
            var containerClient = prefixClient.GetParentBlobContainerClient();
            var prefix = prefixClient.Uri.ToString().Substring(
                0,
                containerClient.Uri.ToString().Length);
            var pageableItems = containerClient.GetBlobsAsync(
                prefix: prefix);
            var blobUris = new List<Uri>();

            await foreach (var item in pageableItems)
            {
                //blobUris.Add(item.Name);
                if (sourceCount != null
                    && blobUris.Count >= sourceCount)
                {
                    break;
                }
            }

            return new DataConnection();
        }
        #endregion
    }
}