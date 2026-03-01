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
/// Additional options for list operations.
/// </summary>
public sealed class ListOptions
{
    public bool Recursive { get; init; }

    public long? Limit { get; init; }

    public string? StartAfter { get; init; }

    public bool Versions { get; init; }

    public bool Deleted { get; init; }

    internal IReadOnlyDictionary<string, string> ToNativeOptions()
    {
        if (Limit is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(Limit), "Limit must be > 0 when provided.");
        }

        var options = new Dictionary<string, string>();

        if (Recursive)
        {
            options["recursive"] = "true";
        }

        if (Limit is not null)
        {
            options["limit"] = Limit.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        if (!string.IsNullOrEmpty(StartAfter))
        {
            options["start_after"] = StartAfter;
        }

        if (Versions)
        {
            options["versions"] = "true";
        }

        if (Deleted)
        {
            options["deleted"] = "true";
        }

        return options;
    }
}
