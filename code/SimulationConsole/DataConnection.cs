using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using System.Collections.Immutable;

namespace SimulationConsole
{
    internal class DataConnection
    {
        private readonly IImmutableList<Uri> _blobUris;

        #region Constructors
        private DataConnection(IImmutableList<Uri> blobUris)
        {
            _blobUris = blobUris;
        }

        public static async Task<DataConnection> CreateDataConnectionAsync(
            Uri sourceBlobPrefixUri,
            int? sourceCount)
        {
            var blobUris = await FetchBlobUrisAsync(sourceBlobPrefixUri, sourceCount);

            return new DataConnection(blobUris.ToImmutableArray());
        }

        private static async Task<IEnumerable<Uri>> FetchBlobUrisAsync(
            Uri sourceBlobPrefixUri,
            int? sourceCount)
        {
            var credentials = new AzureSasCredential(sourceBlobPrefixUri.Query);
            var prefixClient = new BlockBlobClient(
                new Uri(sourceBlobPrefixUri.GetLeftPart(UriPartial.Path)),
                credentials);
            var containerClient = prefixClient.GetParentBlobContainerClient();
            var prefixInContainer = string.Join(
                string.Empty,
                sourceBlobPrefixUri.Segments.Skip(2));
            var pageableItems = containerClient
                .GetBlobsAsync(prefix: prefixInContainer)
                .AsPages(null, sourceCount);
            var blobUris = new List<Uri>();

            await foreach (var page in pageableItems)
            {
                foreach (var item in page.Values)
                {
                    var path = $"{containerClient.Uri}/{item.Name}{sourceBlobPrefixUri.Query}";

                    blobUris.Add(new Uri(path));
                }
                if (sourceCount != null
                    && blobUris.Count >= sourceCount)
                {
                    return blobUris;
                }
            }

            return blobUris;
        }
        #endregion
    }
}