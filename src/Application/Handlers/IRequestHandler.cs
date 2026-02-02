namespace Kafka.Application.Handlers;

using Kafka.Application.Dto;

/// <summary>
/// Interface for Kafka API request handlers.
/// </summary>
public interface IRequestHandler
{
    /// <summary>
    /// The API key this handler processes.
    /// </summary>
    short ApiKey { get; }

    /// <summary>
    /// Handles the request and returns the response bytes.
    /// </summary>
    byte[] Handle(KafkaRequest request);
}
