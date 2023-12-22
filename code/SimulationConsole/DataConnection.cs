using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using System.Collections.Immutable;
using System.Data;

namespace SimulationConsole
{
    internal class DataConnection
    {
        #region Inner Types
        private record BlobInfo(Uri uri, long size);
        #endregion

        private readonly Random _random = new();
        private readonly IImmutableList<BlobInfo> _blobInfos;
        private readonly IIngestionQueue _queue;

        #region Constructors
        private DataConnection(IImmutableList<BlobInfo> blobInfos, IIngestionQueue queue)
        {
            _blobInfos = blobInfos;
            _queue = queue;
        }

        public static async Task<DataConnection> CreateDataConnectionAsync(
            Uri sourceBlobPrefixUri,
            int? sourceCount,
            IIngestionQueue queue)
        {
            var blobUris = await FetchBlobInfosAsync(sourceBlobPrefixUri, sourceCount);

            return new DataConnection(blobUris.ToImmutableArray(), queue);
        }

        private static async Task<IEnumerable<BlobInfo>> FetchBlobInfosAsync(
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
            var blobUris = new List<BlobInfo>();

            await foreach (var page in pageableItems)
            {
                foreach (var item in page.Values)
                {
                    var path = $"{containerClient.Uri}/{item.Name}{sourceBlobPrefixUri.Query}";
                    var info = new BlobInfo(
                        new Uri(path),
                        item.Properties.ContentLength!.Value);

                    blobUris.Add(info);
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

        public async Task RunAsync(TimeSpan duration)
        {
            var startTime = DateTime.Now;
            var endTime = startTime.Add(duration);

            while (DateTime.Now < endTime)
            {
                var blob = RandomBlob();

                _queue.PushUri(blob.uri, blob.size, DateTime.Now.Subtract(RandomAge()));
                await Task.Delay(TimeSpan.FromSeconds(1));
            }
        }

        private BlobInfo RandomBlob()
        {
            var index = _random.Next(_blobInfos.Count());

            return _blobInfos[index];
        }

        private TimeSpan RandomAge()
        {
            var randomValue = _random.NextDouble() * 10;
            var squashedValue = Math.Pow(randomValue, 6) / 1000000 * 3;

            return TimeSpan.FromSeconds(squashedValue);
        }
    }
}