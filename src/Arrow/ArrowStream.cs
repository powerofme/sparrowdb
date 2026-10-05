using System;
using System.IO;

namespace SparrowDb.Arrow;

public static class ArrowStream
{
    private static readonly byte[] ArrowIpcMagic = [0x41, 0x52, 0x38, 0x30, 0x30, 0x31]; // 'A','R','8','0','0','1' binary header indicator

    public static bool HasArrowMagicBytes(byte[] buffer)
    {
        if (buffer == null || buffer.Length < 6)
            return false;

        // Check standard Arrow IPC magic bytes
        return buffer[0] == 0x41 && buffer[1] == 0x52;
    }
}
