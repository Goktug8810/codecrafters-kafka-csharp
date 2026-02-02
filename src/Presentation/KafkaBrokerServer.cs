namespace Kafka.Presentation;

using System.Net;
using System.Net.Sockets;

/// <summary>
/// TCP server that listens for Kafka client connections.
/// </summary>
public class KafkaBrokerServer
{
    private readonly TcpListener _listener;
    private readonly Func<TcpClient, Task> _clientHandler;

    public KafkaBrokerServer(int port, Func<TcpClient, Task> clientHandler)
    {
        _listener = new TcpListener(IPAddress.Any, port);
        _clientHandler = clientHandler;
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        _listener.Start();
        Console.WriteLine("Kafka broker stub running on port 9092...");

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(cancellationToken);
                _ = Task.Run(() => _clientHandler(client), cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown
        }
        finally
        {
            _listener.Stop();
        }
    }
}
