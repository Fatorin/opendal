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
/// Additional options for stat operations.
/// </summary>
public sealed class StatOptions
{
    public string? Version { get; init; }

    public string? IfMatch { get; init; }

    public string? IfNoneMatch { get; init; }

    public DateTimeOffset? IfModifiedSince { get; init; }

    public DateTimeOffset? IfUnmodifiedSince { get; init; }

    public string? OverrideContentType { get; init; }

    public string? OverrideCacheControl { get; init; }

    public string? OverrideContentDisposition { get; init; }

    internal IReadOnlyDictionary<string, string> ToNativeOptions()
    {
        var options = new Dictionary<string, string>();

        if (!string.IsNullOrEmpty(Version))
        {
            options["version"] = Version;
        }

        if (!string.IsNullOrEmpty(IfMatch))
        {
            options["if_match"] = IfMatch;
        }

        if (!string.IsNullOrEmpty(IfNoneMatch))
        {
            options["if_none_match"] = IfNoneMatch;
        }

        if (IfModifiedSince is not null)
        {
            options["if_modified_since"] = IfModifiedSince.Value.ToUnixTimeMilliseconds().ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        if (IfUnmodifiedSince is not null)
        {
            options["if_unmodified_since"] = IfUnmodifiedSince.Value.ToUnixTimeMilliseconds().ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        if (!string.IsNullOrEmpty(OverrideContentType))
        {
            options["override_content_type"] = OverrideContentType;
        }

        if (!string.IsNullOrEmpty(OverrideCacheControl))
        {
            options["override_cache_control"] = OverrideCacheControl;
        }

        if (!string.IsNullOrEmpty(OverrideContentDisposition))
        {
            options["override_content_disposition"] = OverrideContentDisposition;
        }

        return options;
    }
}
