namespace Kafka.Application.Handlers;

using Kafka.Application.Dto;
using Kafka.Infrastructure.Protocol;

/// <summary>
/// Handles ApiVersions (API Key 18) requests.
/// Returns supported API versions.
/// </summary>
public class ApiVersionsHandler : IRequestHandler
{
    public short ApiKey => 18;

    public byte[] Handle(KafkaRequest request)
    {
        short errorCode = (request.ApiVersion < 0 || request.ApiVersion > 4) 
            ? (short)35  // UNSUPPORTED_VERSION
            : (short)0;
        
        return BuildResponse(request.CorrelationId, errorCode);
    }

    private static byte[] BuildResponse(int correlationId, short errorCode)
    {
        var bytes = new List<byte>();

        // Size placeholder
        bytes.AddRange(new byte[4]);

        // Header
        BigEndianWriter.WriteInt32(bytes, correlationId);
        BigEndianWriter.WriteInt16(bytes, errorCode);

        // API entries count (4 APIs) = compact array length = 4 + 1 = 5
        bytes.Add(0x05);

        // (0) Produce API - Key 0, versions 0-11
        BigEndianWriter.WriteInt16(bytes, 0);
        BigEndianWriter.WriteInt16(bytes, 0);
        BigEndianWriter.WriteInt16(bytes, 11);
        bytes.Add(0x00); // tagged fields

        // (1) Fetch API - Key 1, versions 0-16
        BigEndianWriter.WriteInt16(bytes, 1);
        BigEndianWriter.WriteInt16(bytes, 0);
        BigEndianWriter.WriteInt16(bytes, 16);
        bytes.Add(0x00);

        // (2) ApiVersions - Key 18, versions 0-4
        BigEndianWriter.WriteInt16(bytes, 18);
        BigEndianWriter.WriteInt16(bytes, 0);
        BigEndianWriter.WriteInt16(bytes, 4);
        bytes.Add(0x00);

        // (3) DescribeTopicPartitions - Key 75, version 0
        BigEndianWriter.WriteInt16(bytes, 75);
        BigEndianWriter.WriteInt16(bytes, 0);
        BigEndianWriter.WriteInt16(bytes, 0);
        bytes.Add(0x00);

        // throttle_time_ms
        BigEndianWriter.WriteInt32(bytes, 0);

        // response tagged fields
        bytes.Add(0x00);

        // Fix message size
        byte[] result = bytes.ToArray();
        int size = result.Length - 4;
        byte[] sizeBytes = BigEndianWriter.ToBytes(size);
        Array.Copy(sizeBytes, 0, result, 0, 4);

        return result;
    }
}
