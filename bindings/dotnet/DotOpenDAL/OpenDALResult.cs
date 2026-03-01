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

namespace DotOpenDAL;

[StructLayout(LayoutKind.Sequential)]
/// <summary>
/// Result wrapper for operations that only return success or error.
/// </summary>
public struct OpenDALResult
{
    /// <summary>
    /// Error details for the operation.
    /// </summary>
    public OpenDALError Error;
}

[StructLayout(LayoutKind.Sequential)]
/// <summary>
/// Result wrapper for operations that return a native pointer payload.
/// </summary>
internal struct OpenDALPointerResult
{
    /// <summary>
    /// Native pointer payload on success.
    /// </summary>
    public IntPtr Ptr;

    /// <summary>
    /// Error details for the operation.
    /// </summary>
    public OpenDALError Error;
}

[StructLayout(LayoutKind.Sequential)]
/// <summary>
/// Result wrapper for operations that return a byte buffer payload.
/// </summary>
internal struct OpenDALByteBufferResult
{
    /// <summary>
    /// Byte buffer payload on success.
    /// </summary>
    public ByteBuffer Buffer;

    /// <summary>
    /// Error details for the operation.
    /// </summary>
    public OpenDALError Error;
}

[StructLayout(LayoutKind.Sequential)]
/// <summary>
/// Native payload for metadata returned by stat operations.
/// </summary>
internal struct OpenDALMetadata
{
    public int Mode;

    public ulong ContentLength;

    public IntPtr ContentDisposition;

    public IntPtr ContentMd5;

    public IntPtr ContentType;

    public IntPtr ContentEncoding;

    public IntPtr CacheControl;

    public IntPtr ETag;

    public byte LastModifiedHasValue;

    public long LastModifiedSecond;

    public int LastModifiedNanosecond;

    public IntPtr Version;
}

[StructLayout(LayoutKind.Sequential)]
/// <summary>
/// Native entry payload used by list operations.
/// </summary>
internal struct OpenDALEntry
{
    public IntPtr Path;

    public IntPtr Metadata;
}

[StructLayout(LayoutKind.Sequential)]
/// <summary>
/// Native list payload that points to an array of entry pointers.
/// </summary>
internal struct OpenDALEntryList
{
    public IntPtr Entries;

    public nuint Len;
}

[StructLayout(LayoutKind.Sequential)]
/// <summary>
/// Native payload for operator metadata returned by <c>operator_info_get</c>.
/// </summary>
/// <remarks>
/// String fields are unmanaged UTF-8 pointers allocated by native code and are
/// released together via <c>operator_info_free</c>.
/// </remarks>
internal struct OpenDALOperatorInfo
{
    /// <summary>
    /// Backend scheme name pointer.
    /// </summary>
    public IntPtr Scheme;

    /// <summary>
    /// Backend root path pointer.
    /// </summary>
    public IntPtr Root;

    /// <summary>
    /// Backend display name pointer.
    /// </summary>
    public IntPtr Name;

    /// <summary>
    /// Full capability payload.
    /// </summary>
    public Capability FullCapability;

    /// <summary>
    /// Native capability payload.
    /// </summary>
    public Capability NativeCapability;
}