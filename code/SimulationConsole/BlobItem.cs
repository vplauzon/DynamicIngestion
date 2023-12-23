namespace SimulationConsole
{
    public record BlobItem(Uri uri, long size, DateTime eventStart)
    {
        public Guid ItemId { get; } = Guid.NewGuid();
    }
}
