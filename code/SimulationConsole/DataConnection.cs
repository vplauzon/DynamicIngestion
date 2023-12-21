using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using System.Collections.Immutable;
using System.Data;

namespace SimulationConsole
{
    internal class DataConnection
    {
        private readonly Random _random = new();
        private readonly IImmutableList<Uri> _blobUris;
        private readonly IIngestionQueue _queue;

        #region Constructors
        private DataConnection(IImmutableList<Uri> blobUris, IIngestionQueue queue)
        {
            _blobUris = blobUris;
            _queue = queue;
        }

        public static async Task<DataConnection> CreateDataConnectionAsync(
            Uri sourceBlobPrefixUri,
            int? sourceCount,
            IIngestionQueue queue)
        {
            var blobUris = await FetchBlobUrisAsync(sourceBlobPrefixUri, sourceCount);

            return new DataConnection(blobUris.ToImmutableArray(), queue);
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

        public async Task RunAsync(TimeSpan duration)
        {
            var startTime = DateTime.Now;
            var endTime = startTime.Add(duration);

            while (DateTime.Now < endTime)
            {
                _queue.PushUri(RandomUri(), DateTime.Now.Subtract(RandomAge()));
                await Task.Delay(TimeSpan.FromSeconds(1));
            }
        }

        private Uri RandomUri()
        {
            var index = _random.Next(_blobUris.Count());

            return _blobUris[index];
        }

        private TimeSpan RandomAge()
        {
            var randomValue = _random.NextDouble() * 10;
            var squashedValue = Math.Pow(randomValue, 6) / 1000000 * 3;

            return TimeSpan.FromSeconds(squashedValue);
        }
    }
}