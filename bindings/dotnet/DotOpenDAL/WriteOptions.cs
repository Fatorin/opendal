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
/// Additional options for write operations.
/// </summary>
public sealed class WriteOptions
{
    public bool Append { get; init; }

    public string? CacheControl { get; init; }

    public string? ContentType { get; init; }

    public string? ContentDisposition { get; init; }

    public string? ContentEncoding { get; init; }

    public string? IfMatch { get; init; }

    public string? IfNoneMatch { get; init; }

    public bool IfNotExists { get; init; }

    public int Concurrent { get; init; } = 1;

    public long? Chunk { get; init; }

    public IReadOnlyDictionary<string, string>? UserMetadata { get; init; }

    internal IReadOnlyDictionary<string, string> ToNativeOptions()
    {
        if (Concurrent <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(Concurrent), "Concurrent must be > 0.");
        }

        if (Chunk is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(Chunk), "Chunk must be > 0 when provided.");
        }

        var options = new Dictionary<string, string>();

        if (Append)
        {
            options["append"] = "true";
        }

        if (!string.IsNullOrEmpty(CacheControl))
        {
            options["cache_control"] = CacheControl;
        }

        if (!string.IsNullOrEmpty(ContentType))
        {
            options["content_type"] = ContentType;
        }

        if (!string.IsNullOrEmpty(ContentDisposition))
        {
            options["content_disposition"] = ContentDisposition;
        }

        if (!string.IsNullOrEmpty(ContentEncoding))
        {
            options["content_encoding"] = ContentEncoding;
        }

        if (!string.IsNullOrEmpty(IfMatch))
        {
            options["if_match"] = IfMatch;
        }

        if (!string.IsNullOrEmpty(IfNoneMatch))
        {
            options["if_none_match"] = IfNoneMatch;
        }

        if (IfNotExists)
        {
            options["if_not_exists"] = "true";
        }

        if (Concurrent != 1)
        {
            options["concurrent"] = Concurrent.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        if (Chunk is not null)
        {
            options["chunk"] = Chunk.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        if (UserMetadata is not null)
        {
            foreach (var entry in UserMetadata)
            {
                options[$"user_metadata.{entry.Key}"] = entry.Value;
            }
        }

        return options;
    }
}
