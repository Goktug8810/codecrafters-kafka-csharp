namespace Kafka.Application.Handlers;

using Kafka.Application.Dto;
using Kafka.Domain.Services;
using Kafka.Infrastructure.Protocol;

/// <summary>
/// Handles Fetch (API Key 1) requests.
/// Returns records from topic partitions.
/// </summary>
public class FetchHandler : IRequestHandler
{
    private readonly IMetadataService _metadataService;

    public FetchHandler(IMetadataService metadataService)
    {
        _metadataService = metadataService;
    }

    public short ApiKey => 1;

    public byte[] Handle(KafkaRequest request)
    {
        var (topicsCount, topicId, partitionIndex) = ParseRequest(request.RawMessage);

        if (topicsCount == 0)
        {
            return BuildNoTopicsResponse(request.CorrelationId);
        }

        bool exists = _metadataService.TopicIdExists(topicId);

        if (!exists)
        {
            return BuildUnknownTopicResponse(request.CorrelationId, topicId);
        }

        string? topicName = _metadataService.FindTopicNameById(topicId);
        if (topicName == null)
        {
            return BuildUnknownTopicResponse(request.CorrelationId, topicId);
        }

        string dataLogPath = _metadataService.GetDataLogPath(topicName, partitionIndex);
        var fi = new FileInfo(dataLogPath);

        if (!fi.Exists || fi.Length == 0)
        {
            return BuildEmptyTopicResponse(request.CorrelationId, topicId);
        }

        var batches = _metadataService.GetRecordBatches(topicName, partitionIndex);
        return BuildMultipleBatchesResponse(request.CorrelationId, topicId, partitionIndex, batches);
    }

    private static (int topicsCount, Guid topicId, int partitionIndex) ParseRequest(byte[] msg)
    {
        int o = 0;

        // Header v2
        o += 2; // apiKey
        o += 2; // apiVersion
        o += 4; // correlationId

        short clientLen = BigEndianReader.ReadInt16(msg, o);
        o += 2;
        if (clientLen > 0) o += clientLen;

        // header tagged fields
        VarIntCodec.ReadUnsignedVarInt(msg, ref o);

        // Body v16
        o += 4; // max_wait_ms
        o += 4; // min_bytes
        o += 4; // max_bytes
        o += 1; // isolation_level
        o += 4; // session_id
        o += 4; // session_epoch

        int topicsLen = VarIntCodec.ReadUnsignedVarInt(msg, ref o);
        int topicsCount = topicsLen - 1;

        if (topicsCount == 0)
            return (0, Guid.Empty, 0);

        // UUID
        Guid topicId = GuidConverter.ReadGuid(msg, o);
        o += 16;

        // partitions compact array
        int partsLen = VarIntCodec.ReadUnsignedVarInt(msg, ref o);
        int partsCount = partsLen - 1;

        int partitionIndex = 0;
        if (partsCount > 0)
        {
            partitionIndex = BigEndianReader.ReadInt32(msg, o);
        }

        return (topicsCount, topicId, partitionIndex);
    }

    private static byte[] BuildNoTopicsResponse(int correlationId)
    {
        var bytes = new List<byte>();

        // placeholder size
        bytes.AddRange(new byte[4]);

        // header
        BigEndianWriter.WriteInt32(bytes, correlationId);
        bytes.Add(0x00);

        // body
        BigEndianWriter.WriteInt32(bytes, 0); // throttle_time_ms
        BigEndianWriter.WriteInt16(bytes, 0); // error_code
        BigEndianWriter.WriteInt32(bytes, 0); // session_id

        // responses empty
        bytes.Add(0x01);

        // body tagged fields
        bytes.Add(0x00);

        return FixSize(bytes);
    }

    private static byte[] BuildUnknownTopicResponse(int correlationId, Guid topicId)
    {
        var bytes = new List<byte>();

        bytes.AddRange(new byte[4]);

        BigEndianWriter.WriteInt32(bytes, correlationId);
        bytes.Add(0x00);

        BigEndianWriter.WriteInt32(bytes, 0); // throttle_time
        BigEndianWriter.WriteInt16(bytes, 0); // error_code
        BigEndianWriter.WriteInt32(bytes, 0); // session_id

        bytes.Add(0x02); // 1 topic

        bytes.AddRange(GuidConverter.ToBigEndianBytes(topicId));

        bytes.Add(0x02); // 1 partition

        BigEndianWriter.WriteInt32(bytes, 0);  // partition_index
        BigEndianWriter.WriteInt16(bytes, 100); // UNKNOWN_TOPIC_ID
        BigEndianWriter.WriteInt64(bytes, 0);  // high_watermark
        BigEndianWriter.WriteInt64(bytes, 0);  // last_stable_offset
        BigEndianWriter.WriteInt64(bytes, 0);  // log_start_offset

        bytes.Add(0x01); // aborted transactions empty
        BigEndianWriter.WriteInt32(bytes, -1); // preferred_read_replica
        bytes.Add(0x00); // records empty

        bytes.Add(0x00); // partition tagged
        bytes.Add(0x00); // topic tagged
        bytes.Add(0x00); // body tagged

        return FixSize(bytes);
    }

    private static byte[] BuildEmptyTopicResponse(int correlationId, Guid topicId)
    {
        var bytes = new List<byte>();

        bytes.AddRange(new byte[4]);

        BigEndianWriter.WriteInt32(bytes, correlationId);
        bytes.Add(0x00);

        BigEndianWriter.WriteInt32(bytes, 0);
        BigEndianWriter.WriteInt16(bytes, 0);
        BigEndianWriter.WriteInt32(bytes, 0);

        bytes.Add(0x02);

        bytes.AddRange(GuidConverter.ToBigEndianBytes(topicId));

        bytes.Add(0x02);

        BigEndianWriter.WriteInt32(bytes, 0);
        BigEndianWriter.WriteInt16(bytes, 0);
        BigEndianWriter.WriteInt64(bytes, 0);
        BigEndianWriter.WriteInt64(bytes, 0);
        BigEndianWriter.WriteInt64(bytes, 0);

        bytes.Add(0x01);
        BigEndianWriter.WriteInt32(bytes, -1);
        bytes.Add(0x00); // null batch

        bytes.Add(0x00);
        bytes.Add(0x00);
        bytes.Add(0x00);

        return FixSize(bytes);
    }

    private static byte[] BuildMultipleBatchesResponse(
        int correlationId, Guid topicId, int partitionIndex, List<byte[]> batches)
    {
        var bytes = new List<byte>();

        bytes.AddRange(new byte[4]);

        BigEndianWriter.WriteInt32(bytes, correlationId);
        bytes.Add(0x00);

        BigEndianWriter.WriteInt32(bytes, 0); // throttle_time
        BigEndianWriter.WriteInt16(bytes, 0); // error_code
        BigEndianWriter.WriteInt32(bytes, 0); // session_id

        bytes.Add(0x02); // 1 topic

        bytes.AddRange(GuidConverter.ToBigEndianBytes(topicId));

        bytes.Add(0x02); // 1 partition

        BigEndianWriter.WriteInt32(bytes, partitionIndex);
        BigEndianWriter.WriteInt16(bytes, 0); // error=0
        BigEndianWriter.WriteInt64(bytes, 0); // high watermark
        BigEndianWriter.WriteInt64(bytes, 0);
        BigEndianWriter.WriteInt64(bytes, 0);

        bytes.Add(0x01); // aborted transactions empty
        BigEndianWriter.WriteInt32(bytes, -1); // preferred_read_replica

        // RECORDS
        int totalBytes = batches.Sum(b => b.Length);
        VarIntCodec.WriteUnsignedVarInt(bytes, totalBytes + 1);

        foreach (var batch in batches)
            bytes.AddRange(batch);

        bytes.Add(0x00); // partition tagged
        bytes.Add(0x00); // topic tagged
        bytes.Add(0x00); // body tagged

        return FixSize(bytes);
    }

    private static byte[] FixSize(List<byte> bytes)
    {
        byte[] result = bytes.ToArray();
        int size = result.Length - 4;
        byte[] sizeBytes = BigEndianWriter.ToBytes(size);
        Array.Copy(sizeBytes, 0, result, 0, 4);
        return result;
    }
}
