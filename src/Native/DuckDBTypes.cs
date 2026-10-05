using System;
using System.Runtime.InteropServices;

namespace SparrowDb.Native;

public enum DuckDBState
{
    Success = 0,
    Error = 1
}

public enum DuckDBType
{
    Invalid = 0,
    Boolean = 1,
    TinyInt = 2,
    SmallInt = 3,
    Integer = 4,
    BigInt = 5,
    UnsignedTinyInt = 6,
    UnsignedSmallInt = 7,
    UnsignedInteger = 8,
    UnsignedBigInt = 9,
    Float = 10,
    Double = 11,
    Timestamp = 12,
    Date = 13,
    Time = 14,
    Interval = 15,
    HugeInt = 16,
    Varchar = 17,
    Blob = 18,
    Decimal = 19,
    TimestampS = 20,
    TimestampMs = 21,
    TimestampNs = 22,
    Enum = 23,
    List = 24,
    Struct = 25,
    Map = 26,
    Array = 27,
    Uuid = 28,
    Union = 29,
    Bit = 30,
    TimeTz = 31,
    TimestampTz = 32
}

[StructLayout(LayoutKind.Sequential)]
public unsafe struct DuckDBResult
{
    public idx_t ColumnCount;
    public idx_t RowCount;
    public idx_t RowsChanged;
    public DuckDBColumn* Columns;
    public char* ErrorMessage;
    public void* InternalData;
}

[StructLayout(LayoutKind.Sequential)]
public unsafe struct DuckDBColumn
{
    public void* Data;
    public bool* NullMask;
    public DuckDBType Type;
    public char* Name;
    public void* InternalData;
}

public unsafe struct idx_t
{
    public ulong Value;

    public idx_t(ulong value) => Value = value;

    public static implicit operator ulong(idx_t idx) => idx.Value;
    public static implicit operator idx_t(ulong value) => new(value);
    public static implicit operator long(idx_t idx) => (long)idx.Value;
    public static implicit operator idx_t(long value) => new((ulong)value);
}

[StructLayout(LayoutKind.Sequential)]
public unsafe struct ArrowSchema
{
    public sbyte* format;
    public sbyte* name;
    public sbyte* metadata;
    public long flags;
    public long n_children;
    public ArrowSchema** children;
    public ArrowSchema* dictionary;
    public delegate* unmanaged[Cdecl]<ArrowSchema*, void> release;
    public void* private_data;
}

[StructLayout(LayoutKind.Sequential)]
public unsafe struct ArrowArray
{
    public long length;
    public long null_count;
    public long offset;
    public long n_buffers;
    public long n_children;
    public void** buffers;
    public ArrowArray** children;
    public ArrowArray* dictionary;
    public delegate* unmanaged[Cdecl]<ArrowArray*, void> release;
    public void* private_data;
}

[StructLayout(LayoutKind.Sequential)]
public unsafe struct ArrowArrayStream
{
    public delegate* unmanaged[Cdecl]<ArrowArrayStream*, ArrowSchema*, int> get_schema;
    public delegate* unmanaged[Cdecl]<ArrowArrayStream*, ArrowArray*, int> get_next;
    public delegate* unmanaged[Cdecl]<ArrowArrayStream*, sbyte*> get_last_error;
    public delegate* unmanaged[Cdecl]<ArrowArrayStream*, void> release;
    public void* private_data;
}
