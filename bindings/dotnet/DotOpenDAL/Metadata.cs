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

/// <summary>
/// Entry mode of a path.
/// </summary>
public enum EntryMode
{
    File = 0,
    Dir = 1,
    Unknown = 2,
}

/// <summary>
/// Metadata associated with a path.
/// </summary>
public sealed class Metadata
{
    internal Metadata(
        EntryMode mode,
        ulong contentLength,
        string? contentDisposition,
        string? contentMd5,
        string? contentType,
        string? contentEncoding,
        string? cacheControl,
        string? etag,
        DateTimeOffset? lastModified,
        string? version)
    {
        Mode = mode;
        ContentLength = contentLength;
        ContentDisposition = contentDisposition;
        ContentMd5 = contentMd5;
        ContentType = contentType;
        ContentEncoding = contentEncoding;
        CacheControl = cacheControl;
        ETag = etag;
        LastModified = lastModified;
        Version = version;
    }

    public EntryMode Mode { get; }

    public ulong ContentLength { get; }

    public string? ContentDisposition { get; }

    public string? ContentMd5 { get; }

    public string? ContentType { get; }

    public string? ContentEncoding { get; }

    public string? CacheControl { get; }

    public string? ETag { get; }

    public DateTimeOffset? LastModified { get; }

    public string? Version { get; }

    public bool IsFile => Mode == EntryMode.File;

    public bool IsDir => Mode == EntryMode.Dir;

    internal static Metadata FromNativePointer(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
        {
            throw new InvalidOperationException("stat returned null metadata pointer");
        }

        var payload = Marshal.PtrToStructure<OpenDALMetadata>(ptr);

        try
        {
            return FromNativePayload(payload);
        }
        finally
        {
            NativeMethods.metadata_free(ptr);
        }
    }

    internal static Metadata FromNativePayload(OpenDALMetadata payload)
    {
        DateTimeOffset? lastModified = null;
        if (payload.LastModifiedHasValue != 0)
        {
            lastModified = DateTimeOffset.FromUnixTimeSeconds(payload.LastModifiedSecond)
                .AddTicks(payload.LastModifiedNanosecond / 100);
        }

        var mode = payload.Mode switch
        {
            0 => EntryMode.File,
            1 => EntryMode.Dir,
            _ => EntryMode.Unknown,
        };

        static string? ReadNullableUtf8(IntPtr value)
        {
            return value == IntPtr.Zero ? null : Utilities.ReadUtf8(value);
        }

        return new Metadata(
            mode,
            payload.ContentLength,
            ReadNullableUtf8(payload.ContentDisposition),
            ReadNullableUtf8(payload.ContentMd5),
            ReadNullableUtf8(payload.ContentType),
            ReadNullableUtf8(payload.ContentEncoding),
            ReadNullableUtf8(payload.CacheControl),
            ReadNullableUtf8(payload.ETag),
            lastModified,
            ReadNullableUtf8(payload.Version));
    }
}
