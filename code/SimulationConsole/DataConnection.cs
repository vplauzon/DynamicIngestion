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
            var prefixInContainer = string.Join(
                string.Empty,
                sourceBlobPrefixUri.Segments.Skip(2));
            var pageableItems = containerClient.GetBlobsAsync(
                prefix: prefixInContainer);
            var blobUris = new List<Uri>();

            await foreach (var item in pageableItems)
            {
                var name = item.Name;
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