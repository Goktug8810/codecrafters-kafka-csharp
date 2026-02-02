namespace Kafka.Infrastructure.Protocol;

/// <summary>
/// Big-endian byte reading utilities for Kafka protocol.
/// </summary>
public static class BigEndianReader
{
    public static short ReadInt16(byte[] buffer, int offset)
    {
        byte[] temp = new byte[2];
        Array.Copy(buffer, offset, temp, 0, 2);
        if (BitConverter.IsLittleEndian) Array.Reverse(temp);
        return BitConverter.ToInt16(temp, 0);
    }

    public static int ReadInt32(byte[] buffer, int offset)
    {
        byte[] temp = new byte[4];
        Array.Copy(buffer, offset, temp, 0, 4);
        if (BitConverter.IsLittleEndian) Array.Reverse(temp);
        return BitConverter.ToInt32(temp, 0);
    }

    public static long ReadInt64(byte[] buffer, int offset)
    {
        byte[] temp = new byte[8];
        Buffer.BlockCopy(buffer, offset, temp, 0, 8);
        if (BitConverter.IsLittleEndian) Array.Reverse(temp);
        return BitConverter.ToInt64(temp, 0);
    }
}
