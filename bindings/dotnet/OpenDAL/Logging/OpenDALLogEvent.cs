/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System.Runtime.InteropServices;

namespace OpenDAL.Logging;

/// <summary>
/// Severity of a log event, and the threshold used to filter them.
/// </summary>
/// <remarks>
/// The values follow Rust's <c>log::LevelFilter</c>, so a larger value is more
/// verbose and an event is reported when its level is at or below
/// <see cref="OpenDALLogging.MinimumLevel"/>. <see cref="Information"/> and
/// <see cref="Trace"/> are reserved: no event carries them today, but they are
/// usable as a threshold.
/// </remarks>
public enum OpenDALLogLevel : byte
{
    /// <summary>No event reaches the handler.</summary>
    Off = 0,

    /// <summary>Unexpected errors.</summary>
    Error = 1,

    /// <summary>Expected errors, such as reading a key that does not exist.</summary>
    Warning = 2,

    /// <summary>Reserved; no event is reported at this level today.</summary>
    Information = 3,

    /// <summary>Events that carry no error, such as operation start and finish.</summary>
    Debug = 4,

    /// <summary>Reserved; no event is reported at this level today.</summary>
    Trace = 5,
}

/// <summary>
/// The operation a log event describes.
/// </summary>
public enum OpenDALOperation : byte
{
    /// <summary>Retrieve information about the storage service.</summary>
    Info = 0,
    /// <summary>Create a directory.</summary>
    CreateDir = 1,
    /// <summary>Read a file.</summary>
    Read = 2,
    /// <summary>Write to a file.</summary>
    Write = 3,
    /// <summary>Copy a file.</summary>
    Copy = 4,
    /// <summary>Rename a file.</summary>
    Rename = 5,
    /// <summary>Stat a file or directory.</summary>
    Stat = 6,
    /// <summary>Delete files.</summary>
    Delete = 7,
    /// <summary>Get the next entry while listing.</summary>
    List = 8,
    /// <summary>Generate a presigned URL.</summary>
    Presign = 9,

    /// <summary>An operation this binding does not recognize yet.</summary>
    Unknown = 255,
}

/// <summary>
/// Borrowed UTF-8 slice, valid only while the callback that produced it runs.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal readonly struct OpenDALStrRef
{
    public readonly IntPtr Ptr;
    public readonly nuint Len;

    public unsafe ReadOnlySpan<byte> AsSpan()
    {
        return Ptr == IntPtr.Zero
            ? default
            : new ReadOnlySpan<byte>((void*)Ptr, checked((int)Len));
    }
}

[StructLayout(LayoutKind.Sequential)]
internal readonly struct OpenDALLogPairRef
{
    public readonly OpenDALStrRef Key;
    public readonly OpenDALStrRef Value;
}

[StructLayout(LayoutKind.Sequential)]
internal readonly struct OpenDALLogEventRef
{
    public readonly byte Level;
    public readonly byte Operation;
    public readonly byte HasError;
    public readonly int ErrorCode;
    public readonly OpenDALStrRef Scheme;
    public readonly OpenDALStrRef Name;
    public readonly OpenDALStrRef Root;
    public readonly OpenDALStrRef Message;
    public readonly OpenDALStrRef ErrorMessage;
    public readonly nuint PairCount;
    public readonly IntPtr Pairs;
}

/// <summary>
/// A single log event emitted by the native layer.
/// </summary>
/// <remarks>
/// Every span points into memory owned by the native layer and dies when the handler
/// returns. Being a <c>ref struct</c> is what makes the compiler reject storing it,
/// capturing it in a lambda, or carrying it across an <c>await</c>.
/// </remarks>
public readonly ref struct OpenDALLogEvent
{
    // A ref field rather than a raw pointer, so reading the event needs no
    // unsafe context. Only two places below still do: creating a span over
    // native memory, which the runtime offers no safe way to express.
    private readonly ref readonly OpenDALLogEventRef native;

    internal OpenDALLogEvent(ref readonly OpenDALLogEventRef native)
    {
        this.native = ref native;
    }

    /// <summary>Gets the severity of this event.</summary>
    public OpenDALLogLevel Level => (OpenDALLogLevel)native.Level;

    /// <summary>Gets the operation this event describes.</summary>
    public OpenDALOperation Operation =>
        native.Operation <= (byte)OpenDALOperation.Presign
            ? (OpenDALOperation)native.Operation
            : OpenDALOperation.Unknown;

    /// <summary>Gets the UTF-8 service scheme, such as <c>s3</c> or <c>fs</c>.</summary>
    public ReadOnlySpan<byte> Scheme => native.Scheme.AsSpan();

    /// <summary>
    /// Gets the UTF-8 service name, such as a bucket or container. Empty for
    /// services that have no such name, where <see cref="Root"/> identifies the
    /// operator instead.
    /// </summary>
    public ReadOnlySpan<byte> Name => native.Name.AsSpan();

    /// <summary>Gets the UTF-8 root the operator was configured with.</summary>
    public ReadOnlySpan<byte> Root => native.Root.AsSpan();

    /// <summary>
    /// Gets the UTF-8 event message, such as <c>started</c>, <c>finished</c>, or <c>failed</c>.
    /// </summary>
    public ReadOnlySpan<byte> Message => native.Message.AsSpan();

    /// <summary>
    /// Gets the number of context pairs, such as <c>path</c> or <c>range</c>.
    /// </summary>
    public int ContextCount => checked((int)native.PairCount);

    /// <summary>
    /// Gets the UTF-8 context key at the given position.
    /// </summary>
    /// <param name="index">Position, from zero to <see cref="ContextCount"/> minus one.</param>
    /// <returns>The borrowed key.</returns>
    public ReadOnlySpan<byte> GetContextKey(int index)
    {
        return ContextPair(index).Key.AsSpan();
    }

    /// <summary>
    /// Gets the UTF-8 context value at the given position.
    /// </summary>
    /// <param name="index">Position, from zero to <see cref="ContextCount"/> minus one.</param>
    /// <returns>The borrowed value.</returns>
    public ReadOnlySpan<byte> GetContextValue(int index)
    {
        return ContextPair(index).Value.AsSpan();
    }

    /// <summary>
    /// Gets the error attached to this event, when the operation failed.
    /// </summary>
    /// <param name="code">Error code, using the same values as <see cref="OpenDALException.Code"/>.</param>
    /// <param name="errorMessage">Borrowed UTF-8 error message.</param>
    /// <returns><see langword="true"/> when the event carries an error.</returns>
    /// <remarks>
    /// This reports the failure being logged, not a failure of logging itself.
    /// </remarks>
    public bool TryGetError(out ErrorCode code, out ReadOnlySpan<byte> errorMessage)
    {
        if (native.HasError == 0)
        {
            code = default;
            errorMessage = default;
            return false;
        }

        code = (ErrorCode)native.ErrorCode;
        errorMessage = native.ErrorMessage.AsSpan();
        return true;
    }

    private ref readonly OpenDALLogPairRef ContextPair(int index)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, ContextCount);

        return ref Pairs[index];
    }

    private unsafe ReadOnlySpan<OpenDALLogPairRef> Pairs =>
        native.Pairs == IntPtr.Zero
            ? default
            : new ReadOnlySpan<OpenDALLogPairRef>((void*)native.Pairs, ContextCount);
}
