namespace Kafka.Infrastructure.Protocol;

/// <summary>
/// Variable-length integer encoding/decoding for Kafka protocol.
/// Supports both unsigned varints and zigzag-encoded signed varints.
/// </summary>
public static class VarIntCodec
{
    /// <summary>
    /// Reads an unsigned variable-length integer from buffer.
    /// Used for COMPACT_ARRAY lengths, TAG_BUFFER, etc.
    /// </summary>
    public static int ReadUnsignedVarInt(byte[] buffer, ref int offset)
    {
        int value = 0;
        int shift = 0;
        while (true)
        {
            byte b = buffer[offset++];
            value |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        return value;
    }

    /// <summary>
    /// Writes an unsigned variable-length integer to buffer.
    /// </summary>
    public static void WriteUnsignedVarInt(List<byte> buffer, int value)
    {
        while ((value & ~0x7F) != 0)
        {
            buffer.Add((byte)((value & 0x7F) | 0x80));
            value >>= 7;
        }
        buffer.Add((byte)value);
    }

    /// <summary>
    /// Reads a signed variable-length integer using zigzag encoding.
    /// </summary>
    public static int ReadSignedVarInt(byte[] buffer, ref int offset)
    {
        uint value = 0;
        int shift = 0;

        while (true)
        {
            byte b = buffer[offset++];
            value |= (uint)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }

        // ZigZag decode
        return (int)((value >> 1) ^ (uint)-(int)(value & 1));
    }

    /// <summary>
    /// Writes a signed variable-length integer using zigzag encoding.
    /// </summary>
    public static void WriteSignedVarInt(BinaryWriter writer, int value)
    {
        uint v = (uint)((value << 1) ^ (value >> 31)); // Zigzag encoding
        while ((v & ~0x7Fu) != 0)
        {
            writer.Write((byte)((v & 0x7F) | 0x80));
            v >>= 7;
        }
        writer.Write((byte)v);
    }
}
