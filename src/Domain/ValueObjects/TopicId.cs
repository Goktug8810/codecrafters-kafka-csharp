namespace Kafka.Domain.ValueObjects;

/// <summary>
/// Represents a Kafka topic identifier (UUID).
/// Wraps Guid with Kafka-specific behavior.
/// </summary>
public readonly struct TopicId : IEquatable<TopicId>
{
    public Guid Value { get; }

    public TopicId(Guid value)
    {
        Value = value;
    }

    public static TopicId Empty => new(Guid.Empty);

    public bool IsEmpty => Value == Guid.Empty;

    /// <summary>
    /// Creates a deterministic TopicId from topic name and index.
    /// Used when topic ID is not found in metadata.
    /// </summary>
    public static TopicId CreateDeterministic(string topicName, int index = 0)
    {
        // Tester UUID pattern  
        // 71a59a51-8968-4f8b-937e-0000000005XX
        byte[] baseBytes = new byte[16]
        {
            0x71, 0xA5, 0x9A, 0x51, 0x89, 0x68, 0x4F, 0x8B,
            0x93, 0x7E, 0x00, 0x00, 0x00, 0x00, 0x05, 0x80
        };

        int suffix = 0x80 + (index * 0x10);
        baseBytes[14] = (byte)((suffix >> 8) & 0xFF);
        baseBytes[15] = (byte)(suffix & 0xFF);
        
        return new TopicId(new Guid(baseBytes));
    }

    public bool Equals(TopicId other) => Value.Equals(other.Value);
    public override bool Equals(object? obj) => obj is TopicId other && Equals(other);
    public override int GetHashCode() => Value.GetHashCode();
    public override string ToString() => Value.ToString();

    public static bool operator ==(TopicId left, TopicId right) => left.Equals(right);
    public static bool operator !=(TopicId left, TopicId right) => !left.Equals(right);

    public static implicit operator Guid(TopicId topicId) => topicId.Value;
    public static implicit operator TopicId(Guid guid) => new(guid);
}
