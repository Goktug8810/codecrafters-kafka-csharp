namespace Kafka.Domain.Services;

using Kafka.Domain.Entities;
using Kafka.Domain.ValueObjects;

/// <summary>
/// Domain interface for accessing Kafka metadata.
/// </summary>
public interface IMetadataService
{
    /// <summary>
    /// Gets topic information by name.
    /// </summary>
    Topic? GetTopicByName(string name, int partitionLimit = 1);

    /// <summary>
    /// Gets topic information by ID.
    /// </summary>
    Topic? GetTopicById(Guid topicId);

    /// <summary>
    /// Checks if a topic with the given ID exists.
    /// </summary>
    bool TopicIdExists(Guid topicId);

    /// <summary>
    /// Gets record batches for a topic partition.
    /// </summary>
    List<byte[]> GetRecordBatches(string topicName, int partitionIndex);

    /// <summary>
    /// Gets the data log path for a partition.
    /// </summary>
    string GetDataLogPath(string topicName, int partitionIndex);

    /// <summary>
    /// Finds topic name by its ID.
    /// </summary>
    string? FindTopicNameById(Guid topicId);
}
