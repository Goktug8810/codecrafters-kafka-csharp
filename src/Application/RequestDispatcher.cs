namespace Kafka.Application;

using Kafka.Application.Dto;
using Kafka.Application.Handlers;

/// <summary>
/// Routes incoming Kafka requests to the appropriate handler.
/// </summary>
public class RequestDispatcher
{
    private readonly Dictionary<short, IRequestHandler> _handlers;

    public RequestDispatcher(IEnumerable<IRequestHandler> handlers)
    {
        _handlers = handlers.ToDictionary(h => h.ApiKey);
    }

    public byte[] Dispatch(KafkaRequest request)
    {
        if (_handlers.TryGetValue(request.ApiKey, out var handler))
        {
            return handler.Handle(request);
        }

        // Default: return ApiVersions response with no error
        if (_handlers.TryGetValue(18, out var apiVersionsHandler))
        {
            return apiVersionsHandler.Handle(request);
        }

        throw new InvalidOperationException($"No handler for ApiKey {request.ApiKey}");
    }
}
