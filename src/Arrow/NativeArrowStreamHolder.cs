using System;
using System.Buffers;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using SparrowDb.Native;

namespace SparrowDb.Arrow;

/// <summary>
/// Bridges an Apache.Arrow ArrowStreamReader to DuckDB's ArrowArrayStream C Data Interface.
/// Memory buffers are pinned in-place without copying data.
/// </summary>
public sealed unsafe class NativeArrowStreamHolder : IDisposable
{
    private readonly ArrowStreamReader _reader;
    private readonly Schema _schema;
    private ArrowArrayStream* _streamPtr;
    private GCHandle _selfHandle;
    private bool _disposed;
    private string? _lastError;

    private NativeArrowStreamHolder(ArrowStreamReader reader)
    {
        _reader = reader ?? throw new ArgumentNullException(nameof(reader));
        _schema = reader.Schema ?? throw new InvalidOperationException("Reader schema is null");

        _selfHandle = GCHandle.Alloc(this);
        _streamPtr = (ArrowArrayStream*)NativeMemory.AllocZeroed((nuint)sizeof(ArrowArrayStream));

        _streamPtr->get_schema = &GetSchemaCallback;
        _streamPtr->get_next = &GetNextCallback;
        _streamPtr->get_last_error = &GetLastErrorCallback;
        _streamPtr->release = &ReleaseStreamCallback;
        _streamPtr->private_data = (void*)GCHandle.ToIntPtr(_selfHandle);
    }

    public static NativeArrowStreamHolder Create(ArrowStreamReader reader)
    {
        return new NativeArrowStreamHolder(reader);
    }

    public ArrowArrayStream* StreamPtr
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _streamPtr;
        }
    }

    [UnmanagedCallersOnly(CallConvs = new[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    private static int GetSchemaCallback(ArrowArrayStream* stream, SparrowDb.Native.ArrowSchema* outSchema)
    {
        try
        {
            if (stream == null || outSchema == null) return 22; // EINVAL
            var holder = GetHolder(stream);
            if (holder == null) return 22;

            CArrowSchemaExporter.ExportSchema(holder._schema, (CArrowSchema*)outSchema);
            return 0;
        }
        catch (Exception ex)
        {
            var holder = GetHolder(stream);
            if (holder != null) holder._lastError = ex.Message;
            return 1;
        }
    }

    [UnmanagedCallersOnly(CallConvs = new[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    private static int GetNextCallback(ArrowArrayStream* stream, SparrowDb.Native.ArrowArray* outArray)
    {
        try
        {
            if (stream == null || outArray == null) return 22; // EINVAL
            var holder = GetHolder(stream);
            if (holder == null) return 22;

            var batch = holder._reader.ReadNextRecordBatch();
            if (batch == null)
            {
                // End of stream: signal by setting release callback to null
                outArray->release = null;
                return 0;
            }

            ExportRecordBatch(batch, outArray);
            return 0;
        }
        catch (Exception ex)
        {
            var holder = GetHolder(stream);
            if (holder != null) holder._lastError = ex.Message;
            return 1;
        }
    }

    [UnmanagedCallersOnly(CallConvs = new[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    private static sbyte* GetLastErrorCallback(ArrowArrayStream* stream)
    {
        if (stream == null) return null;
        var holder = GetHolder(stream);
        if (holder?._lastError != null)
        {
            return (sbyte*)Marshal.StringToHGlobalAnsi(holder._lastError);
        }
        return null;
    }

    [UnmanagedCallersOnly(CallConvs = new[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    private static void ReleaseStreamCallback(ArrowArrayStream* stream)
    {
        if (stream == null) return;
        if (stream->private_data != null)
        {
            var gch = GCHandle.FromIntPtr((IntPtr)stream->private_data);
            if (gch.IsAllocated)
            {
                var holder = (NativeArrowStreamHolder)gch.Target!;
                holder.DisposeInternal();
            }
        }
    }

    private static NativeArrowStreamHolder? GetHolder(ArrowArrayStream* stream)
    {
        if (stream == null || stream->private_data == null) return null;
        var gch = GCHandle.FromIntPtr((IntPtr)stream->private_data);
        return gch.IsAllocated ? (NativeArrowStreamHolder)gch.Target! : null;
    }

    private static void ExportRecordBatch(RecordBatch batch, SparrowDb.Native.ArrowArray* outArray)
    {
        int colCount = batch.ColumnCount;
        var batchState = new ArrayState(bufCount: 0, childCount: colCount);

        outArray->length = batch.Length;
        outArray->null_count = 0;
        outArray->offset = 0;
        outArray->n_buffers = 0;
        outArray->buffers = null;
        outArray->n_children = colCount;
        outArray->dictionary = null;

        var children = (SparrowDb.Native.ArrowArray**)NativeMemory.AllocZeroed((nuint)(sizeof(SparrowDb.Native.ArrowArray*) * colCount));
        outArray->children = children;
        batchState.ChildrenPtr = children;

        for (int i = 0; i < colCount; i++)
        {
            var colData = batch.Column(i).Data;
            var child = (SparrowDb.Native.ArrowArray*)NativeMemory.AllocZeroed((nuint)sizeof(SparrowDb.Native.ArrowArray));
            children[i] = child;

            ExportArrayData(colData, child);
        }

        var batchGch = GCHandle.Alloc(batchState);
        outArray->private_data = (void*)GCHandle.ToIntPtr(batchGch);
        outArray->release = &ReleaseArrayCallback;
    }

    private static void ExportArrayData(ArrayData data, SparrowDb.Native.ArrowArray* array)
    {
        int bufCount = data.Buffers.Length;
        int childCount = data.Children?.Length ?? 0;
        var state = new ArrayState(bufCount, childCount);

        array->length = data.Length;
        array->null_count = data.NullCount;
        array->offset = data.Offset;
        array->n_buffers = bufCount;
        array->n_children = childCount;
        array->dictionary = null;

        if (bufCount > 0)
        {
            var buffers = (void**)NativeMemory.AllocZeroed((nuint)(sizeof(void*) * bufCount));
            array->buffers = buffers;
            state.BuffersPtr = buffers;

            for (int b = 0; b < bufCount; b++)
            {
                var buf = data.Buffers[b];
                if (buf.IsEmpty || buf.Length == 0)
                {
                    buffers[b] = null;
                }
                else
                {
                    var pin = buf.Memory.Pin();
                    state.Pins[b] = pin;
                    buffers[b] = pin.Pointer;
                }
            }
        }
        else
        {
            array->buffers = null;
        }

        if (childCount > 0)
        {
            var children = (SparrowDb.Native.ArrowArray**)NativeMemory.AllocZeroed((nuint)(sizeof(SparrowDb.Native.ArrowArray*) * childCount));
            array->children = children;
            state.ChildrenPtr = children;

            for (int c = 0; c < childCount; c++)
            {
                var childArray = (SparrowDb.Native.ArrowArray*)NativeMemory.AllocZeroed((nuint)sizeof(SparrowDb.Native.ArrowArray));
                children[c] = childArray;
                ExportArrayData(data.Children![c], childArray);
            }
        }
        else
        {
            array->children = null;
        }

        if (data.Dictionary != null)
        {
            var dictArray = (SparrowDb.Native.ArrowArray*)NativeMemory.AllocZeroed((nuint)sizeof(SparrowDb.Native.ArrowArray));
            array->dictionary = dictArray;
            state.DictionaryPtr = dictArray;
            ExportArrayData(data.Dictionary, dictArray);
        }

        var gch = GCHandle.Alloc(state);
        array->private_data = (void*)GCHandle.ToIntPtr(gch);
        array->release = &ReleaseArrayCallback;
    }

    [UnmanagedCallersOnly(CallConvs = new[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    private static void ReleaseArrayCallback(SparrowDb.Native.ArrowArray* array)
    {
        if (array == null) return;

        if (array->private_data != null)
        {
            var gch = GCHandle.FromIntPtr((IntPtr)array->private_data);
            if (gch.IsAllocated)
            {
                var state = (ArrayState)gch.Target!;

                for (int b = 0; b < state.Pins.Length; b++)
                {
                    state.Pins[b].Dispose();
                }

                if (state.BuffersPtr != null)
                {
                    NativeMemory.Free(state.BuffersPtr);
                    state.BuffersPtr = null;
                }

                if (state.ChildrenPtr != null)
                {
                    for (int c = 0; c < state.ChildCount; c++)
                    {
                        var child = state.ChildrenPtr[c];
                        if (child != null)
                        {
                            if (child->release != null)
                            {
                                child->release(child);
                            }
                            NativeMemory.Free(child);
                        }
                    }
                    NativeMemory.Free(state.ChildrenPtr);
                    state.ChildrenPtr = null;
                }

                if (state.DictionaryPtr != null)
                {
                    if (state.DictionaryPtr->release != null)
                    {
                        state.DictionaryPtr->release(state.DictionaryPtr);
                    }
                    NativeMemory.Free(state.DictionaryPtr);
                    state.DictionaryPtr = null;
                }

                gch.Free();
            }
            array->private_data = null;
        }

        array->release = null;
    }

    private void DisposeInternal()
    {
        if (!_disposed)
        {
            _disposed = true;
            if (_streamPtr != null)
            {
                _streamPtr->release = null;
                NativeMemory.Free(_streamPtr);
                _streamPtr = null;
            }
            if (_selfHandle.IsAllocated)
            {
                _selfHandle.Free();
            }
        }
    }

    public void Dispose()
    {
        DisposeInternal();
        GC.SuppressFinalize(this);
    }

    ~NativeArrowStreamHolder()
    {
        DisposeInternal();
    }

    private sealed class ArrayState
    {
        public MemoryHandle[] Pins { get; }
        public int ChildCount { get; }
        public void** BuffersPtr { get; set; }
        public SparrowDb.Native.ArrowArray** ChildrenPtr { get; set; }
        public SparrowDb.Native.ArrowArray* DictionaryPtr { get; set; }

        public ArrayState(int bufCount, int childCount)
        {
            Pins = new MemoryHandle[bufCount];
            ChildCount = childCount;
        }
    }
}
