namespace Kafka.Infrastructure.Services;

using System.Text;
using Kafka.Domain.Entities;
using Kafka.Domain.Services;
using Kafka.Domain.ValueObjects;
using Kafka.Infrastructure.Protocol;

/// <summary>
/// Implementation of IMetadataService for reading Kafka metadata from log files.
/// </summary>
public class MetadataService : IMetadataService
{
    private readonly string _metadataLogPath;
    private readonly string _logDir;

    public MetadataService(string propertiesPath)
    {
        _metadataLogPath = GetMetadataLogPath(propertiesPath);
        _logDir = GetLogDir(_metadataLogPath);
    }

    public Topic? GetTopicByName(string name, int partitionLimit = 1)
    {
        if (!File.Exists(_metadataLogPath))
        {
            var defaultPartitions = Enumerable.Range(0, partitionLimit)
                .Select(i => new Partition(i))
                .ToList();
            return new Topic(TopicId.Empty, name, defaultPartitions);
        }

        byte[] fileBytes = File.ReadAllBytes(_metadataLogPath);
        byte[] nameBytes = Encoding.UTF8.GetBytes(name);
        int nameIndex = IndexOf(fileBytes, nameBytes);

        if (nameIndex == -1)
        {
            return Topic.CreateUnknown(name);
        }

        Guid topicId = FindUuidNear(fileBytes, nameIndex, name);
        List<int> partitionIds = FindPartitionIdsForUuid(fileBytes, topicId, partitionLimit);

        if (partitionIds.Count == 0)
        {
            partitionIds = Enumerable.Range(0, partitionLimit).ToList();
        }

        var partitions = partitionIds.Select(i => new Partition(i)).ToList();
        return new Topic(new TopicId(topicId), name, partitions);
    }

    public Topic? GetTopicById(Guid topicId)
    {
        string? name = FindTopicNameById(topicId);
        if (name == null) return null;
        return GetTopicByName(name);
    }

    public bool TopicIdExists(Guid topicId)
    {
        if (!File.Exists(_metadataLogPath))
            return false;

        byte[] fileBytes = File.ReadAllBytes(_metadataLogPath);
        byte[] uuidBytes = GuidConverter.ToBigEndianBytes(topicId);

        for (int i = 0; i <= fileBytes.Length - 16; i++)
        {
            bool match = true;
            for (int j = 0; j < 16; j++)
            {
                if (fileBytes[i + j] != uuidBytes[j])
                {
                    match = false;
                    break;
                }
            }
            if (match) return true;
        }

        return false;
    }

    public string? FindTopicNameById(Guid topicId)
    {
        if (!File.Exists(_metadataLogPath))
            return null;

        byte[] fileBytes = File.ReadAllBytes(_metadataLogPath);
        byte[] uuidBytes = GuidConverter.ToBigEndianBytes(topicId);

        for (int i = 0; i <= fileBytes.Length - 16; i++)
        {
            bool match = true;
            for (int j = 0; j < 16; j++)
            {
                if (fileBytes[i + j] != uuidBytes[j])
                {
                    match = false;
                    break;
                }
            }

            if (!match) continue;

            int end = i - 1;
            if (end <= 0) continue;

            int start = end;
            while (start >= 0 && fileBytes[start] >= 0x20 && fileBytes[start] <= 0x7E)
            {
                start--;
            }

            int len = end - start;
            if (len <= 0) continue;

            return Encoding.UTF8.GetString(fileBytes, start + 1, len);
        }

        return null;
    }

    public List<byte[]> GetRecordBatches(string topicName, int partitionIndex)
    {
        string path = GetDataLogPath(topicName, partitionIndex);
        return ReadAllRecordBatches(path);
    }

    public string GetDataLogPath(string topicName, int partitionIndex)
    {
        string folderName = $"{topicName}-{partitionIndex}";
        return Path.Combine(_logDir, folderName, "00000000000000000000.log");
    }

    #region Private Helpers

    private static string GetMetadataLogPath(string propertiesPath)
    {
        if (File.Exists(propertiesPath))
        {
            foreach (var line in File.ReadAllLines(propertiesPath))
            {
                if (line.StartsWith("log.dirs") || line.StartsWith("metadata.log.dir"))
                {
                    string dir = line.Split('=')[1].Trim();
                    if (!dir.EndsWith("/")) dir += "/";
                    string path = Path.Combine(dir, "__cluster_metadata-0", "00000000000000000000.log");
                    if (File.Exists(path))
                        return path;
                }
            }
        }

        string fallback1 = "/app/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
        string fallback2 = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

        if (File.Exists(fallback1)) return fallback1;
        return fallback2;
    }

    private static string GetLogDir(string metadataLogPath)
    {
        if (string.IsNullOrEmpty(metadataLogPath))
            return "/tmp/kraft-combined-logs";

        var metaDir = Path.GetDirectoryName(metadataLogPath);
        if (string.IsNullOrEmpty(metaDir))
            return "/tmp/kraft-combined-logs";

        var logDir = Path.GetDirectoryName(metaDir);
        if (string.IsNullOrEmpty(logDir))
            return "/tmp/kraft-combined-logs";

        return logDir;
    }

    private static int IndexOf(byte[] haystack, byte[] needle)
    {
        for (int i = 0; i <= haystack.Length - needle.Length; i++)
        {
            bool match = true;
            for (int j = 0; j < needle.Length; j++)
            {
                if (haystack[i + j] != needle[j]) { match = false; break; }
            }
            if (match) return i;
        }
        return -1;
    }

    private static Guid FindUuidNear(byte[] bytes, int aroundIndex, string topicName)
    {
        int nameLength = Encoding.UTF8.GetByteCount(topicName);
        int start = aroundIndex + nameLength;
        int end = Math.Min(bytes.Length - 16, start + 64);

        for (int i = start; i < end; i++)
        {
            if (LooksLikeUuid(bytes, i))
            {
                byte[] uuidBytes = new byte[16];
                Buffer.BlockCopy(bytes, i, uuidBytes, 0, 16);
                return new Guid(GuidConverter.FromBigEndianBytes(uuidBytes));
            }
        }
        return Guid.Empty;
    }

    private static bool LooksLikeUuid(byte[] bytes, int offset)
    {
        if (offset + 16 > bytes.Length) return false;
        int zeros = 0;
        for (int i = 0; i < 16; i++)
            if (bytes[offset + i] == 0x00) zeros++;
        return zeros < 14;
    }

    private static List<int> FindPartitionIdsForUuid(byte[] bytes, Guid topicId, int limit)
    {
        if (limit <= 0) limit = 1;
        var ids = new List<int>();
        byte[] uuidBytes = GuidConverter.ToBigEndianBytes(topicId);

        for (int i = 0; i <= bytes.Length - 20; i++)
        {
            bool match = true;
            for (int j = 0; j < 16; j++)
                if (bytes[i + j] != uuidBytes[j]) { match = false; break; }
            if (!match) continue;

            for (int lookahead = 16; lookahead < 128 && i + lookahead + 4 <= bytes.Length; lookahead++)
            {
                int candidate =
                    (bytes[i + lookahead] << 24) |
                    (bytes[i + lookahead + 1] << 16) |
                    (bytes[i + lookahead + 2] << 8) |
                    bytes[i + lookahead + 3];
                if (candidate >= 0 && candidate < 32)
                    ids.Add(candidate);
            }
        }

        ids = ids.Distinct().OrderBy(x => x).ToList();
        if (limit > 0 && ids.Count > limit) ids = ids.Take(limit).ToList();
        if (ids.Count == 0) ids = Enumerable.Range(0, limit).ToList();
        return ids;
    }

    private static List<byte[]> ReadAllRecordBatches(string path)
    {
        var result = new List<byte[]>();

        if (!File.Exists(path))
            return result;

        byte[] file = File.ReadAllBytes(path);
        int pos = 0;

        while (pos + 12 <= file.Length)
        {
            int batchLength = BigEndianReader.ReadInt32(file, pos + 8);
            int totalLen = 12 + batchLength;

            if (pos + totalLen > file.Length)
                break;

            byte[] batch = new byte[totalLen];
            Buffer.BlockCopy(file, pos, batch, 0, totalLen);
            result.Add(batch);

            pos += totalLen;
        }

        return result;
    }

    #endregion
}
