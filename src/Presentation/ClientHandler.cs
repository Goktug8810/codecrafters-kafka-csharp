namespace Kafka.Presentation;

using System.Net.Sockets;
using Kafka.Application;
using Kafka.Application.Dto;
using Kafka.Infrastructure.Protocol;

/// <summary>
/// Handles individual client connections.
/// Reads requests, dispatches to handlers, sends responses.
/// </summary>
public class ClientHandler
{
    private readonly RequestDispatcher _dispatcher;

    public ClientHandler(RequestDispatcher dispatcher)
    {
        _dispatcher = dispatcher;
    }

    public async Task HandleAsync(TcpClient client)
    {
        Console.WriteLine("Client connected.");

        using (client)
        using (var stream = client.GetStream())
        {
            try
            {
                while (true)
                {
                    // Read message size (4 bytes, big-endian)
                    byte[] sizeBuf = await ReadExactlyAsync(stream, 4);
                    int messageSize = BigEndianReader.ReadInt32(sizeBuf, 0);

                    // Read full request
                    byte[] msg = await ReadExactlyAsync(stream, messageSize);

                    // Parse header
                    var request = ParseRequest(msg);

                    // Dispatch to handler
                    byte[] response = _dispatcher.Dispatch(request);

                    // Send response
                    await stream.WriteAsync(response, 0, response.Length);
                    await stream.FlushAsync();

                    Console.WriteLine($"Sent {response.Length} bytes for correlation_id={request.CorrelationId}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Client disconnected or error: {ex.Message}");
            }
        }
    }

    private static KafkaRequest ParseRequest(byte[] msg)
    {
        return new KafkaRequest
        {
            ApiKey = BigEndianReader.ReadInt16(msg, 0),
            ApiVersion = BigEndianReader.ReadInt16(msg, 2),
            CorrelationId = BigEndianReader.ReadInt32(msg, 4),
            RawMessage = msg
        };
    }

    private static async Task<byte[]> ReadExactlyAsync(NetworkStream stream, int length)
    {
        byte[] buf = new byte[length];
        int offset = 0;

        while (offset < length)
        {
            int n = await stream.ReadAsync(buf, offset, length - offset);
            if (n == 0) throw new Exception("Connection closed early");
            offset += n;
        }

        return buf;
    }
}
