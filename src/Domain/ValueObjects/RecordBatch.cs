namespace Kafka.Domain.ValueObjects;

/// <summary>
/// Represents a Kafka record batch with all its metadata.
/// </summary>
public class RecordBatch
{
    public int PartitionLeaderEpoch { get; init; }
    public byte Magic { get; init; }
    public int CRC { get; init; }
    public short Attributes { get; init; }
    public int LastOffsetDelta { get; init; }
    public long FirstTimestamp { get; init; }
    public long MaxTimestamp { get; init; }
    public long ProducerId { get; init; }
    public short ProducerEpoch { get; init; }
    public int BaseSequence { get; init; }
    public byte[] RecordBytes { get; init; } = Array.Empty<byte>();
}
