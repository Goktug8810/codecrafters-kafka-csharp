namespace Kafka.Infrastructure.Protocol;

/// <summary>
/// Big-endian byte writing utilities for Kafka protocol.
/// </summary>
public static class BigEndianWriter
{
    public static byte[] ToBytes(short value)
    {
        byte[] bytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian) Array.Reverse(bytes);
        return bytes;
    }

    public static byte[] ToBytes(int value)
    {
        byte[] bytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian) Array.Reverse(bytes);
        return bytes;
    }

    public static byte[] ToBytes(long value)
    {
        byte[] bytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian) Array.Reverse(bytes);
        return bytes;
    }

    public static void WriteInt16(List<byte> buffer, short value)
    {
        buffer.AddRange(ToBytes(value));
    }

    public static void WriteInt32(List<byte> buffer, int value)
    {
        buffer.AddRange(ToBytes(value));
    }

    public static void WriteInt64(List<byte> buffer, long value)
    {
        buffer.AddRange(ToBytes(value));
    }
}
