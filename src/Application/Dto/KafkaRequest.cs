namespace Kafka.Application.Dto;

/// <summary>
/// Represents a parsed Kafka request.
/// </summary>
public class KafkaRequest
{
    public short ApiKey { get; init; }
    public short ApiVersion { get; init; }
    public int CorrelationId { get; init; }
    public string? ClientId { get; init; }
    public byte[] RawMessage { get; init; } = Array.Empty<byte>();
}
