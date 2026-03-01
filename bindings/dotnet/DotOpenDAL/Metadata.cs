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
}
