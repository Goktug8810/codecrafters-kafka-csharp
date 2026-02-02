namespace Kafka.Application.Handlers;

using System.Text;
using Kafka.Application.Dto;
using Kafka.Domain.Services;
using Kafka.Infrastructure.Protocol;

/// <summary>
/// Handles Produce (API Key 0) requests.
/// Writes records to topic partitions.
/// </summary>
public class ProduceHandler : IRequestHandler
{
    private readonly IMetadataService _metadataService;

    public ProduceHandler(IMetadataService metadataService)
    {
        _metadataService = metadataService;
    }

    public short ApiKey => 0;

    public byte[] Handle(KafkaRequest request)
    {
        var topics = ParseRequest(request.RawMessage);

        if (topics.Count == 1 && topics[0].Partitions.Count == 1)
        {
            return HandleSinglePartition(request.CorrelationId, topics[0]);
        }

        return HandleMultiplePartitions(request.CorrelationId, topics);
    }

    private byte[] HandleSinglePartition(int correlationId, ProduceTopic topic)
    {
        var partition = topic.Partitions[0];
        var existingTopic = _metadataService.GetTopicByName(topic.Name, 32);

        bool topicExists = existingTopic != null && !existingTopic.IsUnknown;
        bool partitionExists = topicExists && 
            existingTopic!.Partitions.Any(p => p.Index == partition.Index);

        if (partitionExists && partition.RecordBytes.Length > 0)
        {
            WriteToLog(topic.Name, partition.Index, partition.RecordBytes);
            return BuildSuccessResponse(correlationId, topic.Name, partition.Index);
        }

        return BuildInvalidResponse(correlationId, topic.Name, partition.Index);
    }

    private byte[] HandleMultiplePartitions(int correlationId, List<ProduceTopic> topics)
    {
        var responseTopics = new List<(string Name, List<int> PartitionIndexes)>();

        foreach (var topic in topics)
        {
            var partitionIndexes = new List<int>();

            foreach (var partition in topic.Partitions)
            {
                if (partition.RecordBytes.Length > 0)
                {
                    WriteToLog(topic.Name, partition.Index, partition.RecordBytes);
                }
                partitionIndexes.Add(partition.Index);
            }

            responseTopics.Add((topic.Name, partitionIndexes));
        }

        return BuildMultiTopicResponse(correlationId, responseTopics);
    }

    private void WriteToLog(string topicName, int partitionIndex, byte[] recordBytes)
    {
        string dataLogPath = _metadataService.GetDataLogPath(topicName, partitionIndex);
        string? dir = Path.GetDirectoryName(dataLogPath);

        if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
            Directory.CreateDirectory(dir);

        using var fs = new FileStream(dataLogPath, FileMode.Append, FileAccess.Write, FileShare.Read);
        fs.Write(recordBytes, 0, recordBytes.Length);
    }

    #region Request Parsing

    private static List<ProduceTopic> ParseRequest(byte[] msg)
    {
        int o = 0;

        // Header v2
        o += 2; // apiKey
        o += 2; // apiVersion
        o += 4; // correlationId

        short clientLen = BigEndianReader.ReadInt16(msg, o);
        o += 2;
        if (clientLen > 0) o += clientLen;

        VarIntCodec.ReadUnsignedVarInt(msg, ref o); // header tagged fields

        // Body v11
        int txLenPlus1 = VarIntCodec.ReadUnsignedVarInt(msg, ref o);
        if (txLenPlus1 > 0) o += txLenPlus1 - 1; // transactional_id

        o += 2; // acks
        o += 4; // timeout_ms

        int topicsLenPlus1 = VarIntCodec.ReadUnsignedVarInt(msg, ref o);
        int topicsCount = topicsLenPlus1 - 1;

        var result = new List<ProduceTopic>();
        if (topicsCount <= 0) return result;

        for (int t = 0; t < topicsCount; t++)
        {
            int nameLenPlus1 = VarIntCodec.ReadUnsignedVarInt(msg, ref o);
            int nameLen = nameLenPlus1 - 1;
            string topicName = Encoding.UTF8.GetString(msg, o, nameLen);
            o += nameLen;

            int partsLenPlus1 = VarIntCodec.ReadUnsignedVarInt(msg, ref o);
            int partsCount = partsLenPlus1 - 1;

            var partitions = new List<ProducePartition>();

            for (int p = 0; p < partsCount; p++)
            {
                int partitionIndex = BigEndianReader.ReadInt32(msg, o);
                o += 4;

                int recordsLenPlus1 = VarIntCodec.ReadUnsignedVarInt(msg, ref o);
                byte[] recordBytes;

                if (recordsLenPlus1 == 0)
                {
                    recordBytes = Array.Empty<byte>();
                }
                else
                {
                    int recordsLen = recordsLenPlus1 - 1;
                    recordBytes = new byte[recordsLen];
                    Buffer.BlockCopy(msg, o, recordBytes, 0, recordsLen);
                    o += recordsLen;
                }

                VarIntCodec.ReadUnsignedVarInt(msg, ref o); // partition tagged

                partitions.Add(new ProducePartition { Index = partitionIndex, RecordBytes = recordBytes });
            }

            VarIntCodec.ReadUnsignedVarInt(msg, ref o); // topic tagged

            result.Add(new ProduceTopic { Name = topicName, Partitions = partitions });
        }

        return result;
    }

    #endregion

    #region Response Building

    private static byte[] BuildSuccessResponse(int correlationId, string topicName, int partitionIndex)
    {
        var bytes = new List<byte>();
        bytes.AddRange(new byte[4]);

        BigEndianWriter.WriteInt32(bytes, correlationId);
        VarIntCodec.WriteUnsignedVarInt(bytes, 0); // header tagged

        VarIntCodec.WriteUnsignedVarInt(bytes, 2); // 1 topic

        var nameBytes = Encoding.UTF8.GetBytes(topicName);
        VarIntCodec.WriteUnsignedVarInt(bytes, nameBytes.Length + 1);
        bytes.AddRange(nameBytes);

        VarIntCodec.WriteUnsignedVarInt(bytes, 2); // 1 partition

        BigEndianWriter.WriteInt32(bytes, partitionIndex);
        BigEndianWriter.WriteInt16(bytes, 0);  // error = 0
        BigEndianWriter.WriteInt64(bytes, 0);  // base_offset
        BigEndianWriter.WriteInt64(bytes, -1); // log_append_time
        BigEndianWriter.WriteInt64(bytes, 0);  // log_start_offset

        VarIntCodec.WriteUnsignedVarInt(bytes, 1); // record_errors empty
        VarIntCodec.WriteUnsignedVarInt(bytes, 0); // error_message null
        VarIntCodec.WriteUnsignedVarInt(bytes, 0); // partition tagged

        VarIntCodec.WriteUnsignedVarInt(bytes, 0); // topic tagged

        BigEndianWriter.WriteInt32(bytes, 0); // throttle_time
        VarIntCodec.WriteUnsignedVarInt(bytes, 0); // body tagged

        return FixSize(bytes);
    }

    private static byte[] BuildInvalidResponse(int correlationId, string topicName, int partitionIndex)
    {
        var bytes = new List<byte>();
        bytes.AddRange(new byte[4]);

        BigEndianWriter.WriteInt32(bytes, correlationId);
        VarIntCodec.WriteUnsignedVarInt(bytes, 0);

        VarIntCodec.WriteUnsignedVarInt(bytes, 2);

        var nameBytes = Encoding.UTF8.GetBytes(topicName);
        VarIntCodec.WriteUnsignedVarInt(bytes, nameBytes.Length + 1);
        bytes.AddRange(nameBytes);

        VarIntCodec.WriteUnsignedVarInt(bytes, 2);

        BigEndianWriter.WriteInt32(bytes, partitionIndex);
        BigEndianWriter.WriteInt16(bytes, 3);   // UNKNOWN_TOPIC_OR_PARTITION
        BigEndianWriter.WriteInt64(bytes, -1);
        BigEndianWriter.WriteInt64(bytes, -1);
        BigEndianWriter.WriteInt64(bytes, -1);

        VarIntCodec.WriteUnsignedVarInt(bytes, 1);
        VarIntCodec.WriteUnsignedVarInt(bytes, 0);
        VarIntCodec.WriteUnsignedVarInt(bytes, 0);

        VarIntCodec.WriteUnsignedVarInt(bytes, 0);

        BigEndianWriter.WriteInt32(bytes, 0);
        VarIntCodec.WriteUnsignedVarInt(bytes, 0);

        return FixSize(bytes);
    }

    private static byte[] BuildMultiTopicResponse(
        int correlationId, 
        List<(string Name, List<int> PartitionIndexes)> topics)
    {
        var bytes = new List<byte>();
        bytes.AddRange(new byte[4]);

        BigEndianWriter.WriteInt32(bytes, correlationId);
        VarIntCodec.WriteUnsignedVarInt(bytes, 0);

        VarIntCodec.WriteUnsignedVarInt(bytes, topics.Count + 1);

        foreach (var (topicName, partitionIndexes) in topics)
        {
            var nameBytes = Encoding.UTF8.GetBytes(topicName);
            VarIntCodec.WriteUnsignedVarInt(bytes, nameBytes.Length + 1);
            bytes.AddRange(nameBytes);

            VarIntCodec.WriteUnsignedVarInt(bytes, partitionIndexes.Count + 1);

            foreach (var pIndex in partitionIndexes)
            {
                BigEndianWriter.WriteInt32(bytes, pIndex);
                BigEndianWriter.WriteInt16(bytes, 0);
                BigEndianWriter.WriteInt64(bytes, 0);
                BigEndianWriter.WriteInt64(bytes, -1);
                BigEndianWriter.WriteInt64(bytes, 0);

                VarIntCodec.WriteUnsignedVarInt(bytes, 1);
                VarIntCodec.WriteUnsignedVarInt(bytes, 0);
                VarIntCodec.WriteUnsignedVarInt(bytes, 0);
            }

            VarIntCodec.WriteUnsignedVarInt(bytes, 0);
        }

        BigEndianWriter.WriteInt32(bytes, 0);
        VarIntCodec.WriteUnsignedVarInt(bytes, 0);

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

    #endregion

    #region DTOs

    private class ProduceTopic
    {
        public string Name { get; init; } = string.Empty;
        public List<ProducePartition> Partitions { get; init; } = new();
    }

    private class ProducePartition
    {
        public int Index { get; init; }
        public byte[] RecordBytes { get; init; } = Array.Empty<byte>();
    }

    #endregion
}
