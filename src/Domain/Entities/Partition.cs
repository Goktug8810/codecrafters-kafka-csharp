namespace Kafka.Domain.Entities;

using Kafka.Domain.ValueObjects;

/// <summary>
/// Represents a Kafka partition within a topic.
/// </summary>
public class Partition
{
    public int Index { get; }
    public int LeaderId { get; init; }
    public int LeaderEpoch { get; init; }
    public List<int> Replicas { get; init; } = new() { 0 };
    public List<int> Isr { get; init; } = new() { 0 };

    public Partition(int index)
    {
        Index = index;
    }
}
