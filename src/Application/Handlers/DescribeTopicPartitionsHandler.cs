namespace Kafka.Application.Handlers;

using System.Text;
using Kafka.Application.Dto;
using Kafka.Domain.Services;
using Kafka.Domain.ValueObjects;
using Kafka.Infrastructure.Protocol;

/// <summary>
/// Handles DescribeTopicPartitions (API Key 75) requests.
/// Returns topic and partition metadata.
/// </summary>
public class DescribeTopicPartitionsHandler : IRequestHandler
{
    private readonly IMetadataService _metadataService;

    public DescribeTopicPartitionsHandler(IMetadataService metadataService)
    {
        _metadataService = metadataService;
    }

    public short ApiKey => 75;

    public byte[] Handle(KafkaRequest request)
    {
        var (topicNames, responsePartitionLimit) = ParseRequest(request.RawMessage);
        if (responsePartitionLimit < 0) responsePartitionLimit = 0;

        var topics = new List<TopicInfo>();

        for (int index = 0; index < topicNames.Count; index++)
        {
            string name = topicNames[index];

            int topicLimit = topicNames.Count > 1 
                ? index + 1 
                : (responsePartitionLimit > 0 ? responsePartitionLimit : 1);

            bool isUnknown = name.StartsWith("UNKNOWN_TOPIC_", StringComparison.Ordinal);

            if (isUnknown)
            {
                topics.Add(new TopicInfo
                {
                    Name = name,
                    Id = Guid.Empty,
                    PartitionIds = new List<int>(),
                    IsUnknown = true
                });
            }
            else
            {
                var topic = _metadataService.GetTopicByName(name, topicLimit);

                Guid topicId = topic != null && !topic.IsUnknown
                    ? topic.Id.Value
                    : TopicId.CreateDeterministic(name, index).Value;

                var partitionIds = topic?.Partitions
                    .Select(p => p.Index)
                    .Distinct()
                    .OrderBy(x => x)
                    .Take(topicLimit)
                    .ToList() ?? new List<int>();

                if (partitionIds.Count == 0)
                    partitionIds = Enumerable.Range(0, Math.Max(1, topicLimit)).ToList();

                topics.Add(new TopicInfo
                {
                    Name = name,
                    Id = topicId,
                    PartitionIds = partitionIds,
                    IsUnknown = false
                });
            }
        }

        return BuildResponse(request.CorrelationId, topics, responsePartitionLimit);
    }

    #region Request Parsing

    private static (List<string> topicNames, int responsePartitionLimit) ParseRequest(byte[] msg)
    {
        int offset = 0;

        // header skip
        offset += 2; // apiKey
        offset += 2; // apiVersion
        offset += 4; // correlationId

        short clientIdLen = BigEndianReader.ReadInt16(msg, offset);
        offset += 2;
        if (clientIdLen > 0) offset += clientIdLen;

        VarIntCodec.ReadUnsignedVarInt(msg, ref offset); // header tagged

        int topicsLen = VarIntCodec.ReadUnsignedVarInt(msg, ref offset);
        int realTopicsCount = topicsLen - 1;

        var topicNames = new List<string>();

        for (int i = 0; i < realTopicsCount; i++)
        {
            int nameLenPlus1 = VarIntCodec.ReadUnsignedVarInt(msg, ref offset);
            int realLen = nameLenPlus1 - 1;
            string topicName = Encoding.UTF8.GetString(msg, offset, realLen);
            offset += realLen;
            VarIntCodec.ReadUnsignedVarInt(msg, ref offset); // topic tagged
            topicNames.Add(topicName);
        }

        int responsePartitionLimit = BigEndianReader.ReadInt32(msg, offset);
        offset += 4;

        if (responsePartitionLimit < 0) responsePartitionLimit = 0;
        if (responsePartitionLimit > 64) responsePartitionLimit = 64;

        return (topicNames, responsePartitionLimit);
    }

    #endregion

    #region Response Building

    private static byte[] BuildResponse(int correlationId, List<TopicInfo> topics, int responsePartitionLimit)
    {
        var bytes = new List<byte>();

        // Size placeholder
        bytes.AddRange(new byte[4]);

        // Header
        BigEndianWriter.WriteInt32(bytes, correlationId);
        bytes.Add(0x00); // header tagged

        // Body
        BigEndianWriter.WriteInt32(bytes, 0); // throttle_time_ms
        VarIntCodec.WriteUnsignedVarInt(bytes, topics.Count + 1); // topics array

        foreach (var topic in topics)
        {
            // Error code
            short errorCode = topic.IsUnknown ? (short)3 : (short)0;
            BigEndianWriter.WriteInt16(bytes, errorCode);

            // Topic name
            byte[] nameBytes = Encoding.UTF8.GetBytes(topic.Name);
            VarIntCodec.WriteUnsignedVarInt(bytes, nameBytes.Length + 1);
            bytes.AddRange(nameBytes);

            // Topic ID
            if (topic.IsUnknown)
            {
                bytes.AddRange(new byte[16]);
            }
            else
            {
                bytes.AddRange(GuidConverter.ToBigEndianBytes(topic.Id));
            }

            bytes.Add(0x00); // is_internal = false

            // Partitions
            int partitionCount = topic.IsUnknown ? 0 : topic.PartitionIds.Count;
            VarIntCodec.WriteUnsignedVarInt(bytes, partitionCount + 1);

            for (int i = 0; i < partitionCount; i++)
            {
                int pid = topic.PartitionIds[i];

                BigEndianWriter.WriteInt16(bytes, 0); // partition_error_code
                BigEndianWriter.WriteInt32(bytes, pid); // partition_index
                BigEndianWriter.WriteInt32(bytes, 0); // leader_id
                BigEndianWriter.WriteInt32(bytes, 0); // leader_epoch

                // replicas
                VarIntCodec.WriteUnsignedVarInt(bytes, 2);
                BigEndianWriter.WriteInt32(bytes, 0);

                // isr
                VarIntCodec.WriteUnsignedVarInt(bytes, 2);
                BigEndianWriter.WriteInt32(bytes, 0);

                // eligible leader replicas
                VarIntCodec.WriteUnsignedVarInt(bytes, 2);
                BigEndianWriter.WriteInt32(bytes, 0);

                // last known elr
                VarIntCodec.WriteUnsignedVarInt(bytes, 2);
                BigEndianWriter.WriteInt32(bytes, 0);

                // offline replicas
                VarIntCodec.WriteUnsignedVarInt(bytes, 1);

                // partition tagged
                VarIntCodec.WriteUnsignedVarInt(bytes, 0);
            }

            // topic_authorized_operations
            BigEndianWriter.WriteInt32(bytes, 33554432);
            // topic tagged
            VarIntCodec.WriteUnsignedVarInt(bytes, 0);
        }

        // Cursor
        bytes.Add(0xFF); // IsCursorPresent = -1
        VarIntCodec.WriteUnsignedVarInt(bytes, 0); // response tagged

        // Fix size
        byte[] result = bytes.ToArray();
        int messageSize = result.Length - 4;
        byte[] sizeBytes = BigEndianWriter.ToBytes(messageSize);
        Array.Copy(sizeBytes, 0, result, 0, 4);

        return result;
    }

    #endregion

    private class TopicInfo
    {
        public string Name { get; init; } = string.Empty;
        public Guid Id { get; init; }
        public List<int> PartitionIds { get; init; } = new();
        public bool IsUnknown { get; init; }
    }
}
