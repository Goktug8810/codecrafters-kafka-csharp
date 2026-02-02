namespace Kafka.Domain.Entities;

using Kafka.Domain.ValueObjects;

/// <summary>
/// Represents a Kafka topic with its partitions.
/// </summary>
public class Topic
{
    public TopicId Id { get; }
    public string Name { get; }
    public IReadOnlyList<Partition> Partitions { get; }
    public bool IsInternal { get; init; }

    public Topic(TopicId id, string name, IReadOnlyList<Partition> partitions)
    {
        Id = id;
        Name = name;
        Partitions = partitions;
    }

    public static Topic CreateUnknown(string name)
    {
        return new Topic(TopicId.Empty, name, Array.Empty<Partition>());
    }

    public bool IsUnknown => Id.IsEmpty;
}
