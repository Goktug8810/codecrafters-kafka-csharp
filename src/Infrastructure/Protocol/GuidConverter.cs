namespace Kafka.Infrastructure.Protocol;

/// <summary>
/// Converts between .NET Guid (little-endian) and Kafka UUID (big-endian).
/// </summary>
public static class GuidConverter
{
    /// <summary>
    /// Converts .NET Guid to Kafka big-endian UUID bytes.
    /// </summary>
    public static byte[] ToBigEndianBytes(Guid guid)
    {
        byte[] le = guid.ToByteArray();
        byte[] be = new byte[16];
        
        // Convert first 4 bytes (int32)
        be[0] = le[3]; be[1] = le[2]; be[2] = le[1]; be[3] = le[0];
        // Convert next 2 bytes (int16)
        be[4] = le[5]; be[5] = le[4];
        // Convert next 2 bytes (int16)
        be[6] = le[7]; be[7] = le[6];
        // Last 8 bytes remain same
        Array.Copy(le, 8, be, 8, 8);
        
        return be;
    }

    /// <summary>
    /// Converts Kafka big-endian UUID bytes to .NET Guid layout.
    /// </summary>
    public static byte[] FromBigEndianBytes(byte[] be)
    {
        byte[] le = new byte[16];
        
        // Convert first 4 bytes (int32)
        le[0] = be[3]; le[1] = be[2]; le[2] = be[1]; le[3] = be[0];
        // Convert next 2 bytes (int16)
        le[4] = be[5]; le[5] = be[4];
        // Convert next 2 bytes (int16)
        le[6] = be[7]; le[7] = be[6];
        // Last 8 bytes remain same
        Array.Copy(be, 8, le, 8, 8);
        
        return le;
    }

    /// <summary>
    /// Reads a Kafka UUID from byte array and returns as .NET Guid.
    /// </summary>
    public static Guid ReadGuid(byte[] buffer, int offset)
    {
        byte[] be = new byte[16];
        Buffer.BlockCopy(buffer, offset, be, 0, 16);
        return new Guid(FromBigEndianBytes(be));
    }
}
