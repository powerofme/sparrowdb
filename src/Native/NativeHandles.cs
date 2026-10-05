using System;
using System.Runtime.InteropServices;

namespace SparrowDb.Native;

public sealed class DatabaseSafeHandle : SafeHandle
{
    public DatabaseSafeHandle() : base(IntPtr.Zero, ownsHandle: true) { }

    public override bool IsInvalid => handle == IntPtr.Zero;

    protected override bool ReleaseHandle()
    {
        if (!IsInvalid)
        {
            DuckDBNative.duckdb_close(ref handle);
            handle = IntPtr.Zero;
        }
        return true;
    }
}

public sealed class ConnectionSafeHandle : SafeHandle
{
    public ConnectionSafeHandle() : base(IntPtr.Zero, ownsHandle: true) { }

    public override bool IsInvalid => handle == IntPtr.Zero;

    protected override bool ReleaseHandle()
    {
        if (!IsInvalid)
        {
            DuckDBNative.duckdb_disconnect(ref handle);
            handle = IntPtr.Zero;
        }
        return true;
    }
}

public sealed class PreparedStatementSafeHandle : SafeHandle
{
    public PreparedStatementSafeHandle() : base(IntPtr.Zero, ownsHandle: true) { }

    public override bool IsInvalid => handle == IntPtr.Zero;

    protected override bool ReleaseHandle()
    {
        if (!IsInvalid)
        {
            DuckDBNative.duckdb_destroy_prepare(ref handle);
            handle = IntPtr.Zero;
        }
        return true;
    }
}

public sealed class AppenderSafeHandle : SafeHandle
{
    public AppenderSafeHandle() : base(IntPtr.Zero, ownsHandle: true) { }

    public override bool IsInvalid => handle == IntPtr.Zero;

    protected override bool ReleaseHandle()
    {
        if (!IsInvalid)
        {
            DuckDBNative.duckdb_appender_destroy(ref handle);
            handle = IntPtr.Zero;
        }
        return true;
    }
}
