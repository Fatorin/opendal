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
/// Listed entry with path and metadata.
/// </summary>
public sealed class Entry
{
    internal Entry(string path, Metadata metadata)
    {
        Path = path;
        Metadata = metadata;
    }

    public string Path { get; }

    public Metadata Metadata { get; }

    internal static IReadOnlyList<Entry> FromNativePointer(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
        {
            return Array.Empty<Entry>();
        }

        var payload = Marshal.PtrToStructure<OpenDALEntryList>(ptr);
        var count = checked((int)payload.Len);
        var results = new List<Entry>(count);

        try
        {
            for (var index = 0; index < count; index++)
            {
                var entryPtr = Marshal.ReadIntPtr(payload.Entries, index * IntPtr.Size);
                if (entryPtr == IntPtr.Zero)
                {
                    continue;
                }

                var entryPayload = Marshal.PtrToStructure<OpenDALEntry>(entryPtr);
                if (entryPayload.Metadata == IntPtr.Zero)
                {
                    continue;
                }

                var path = Utilities.ReadUtf8(entryPayload.Path);
                var metadataPayload = Marshal.PtrToStructure<OpenDALMetadata>(entryPayload.Metadata);
                results.Add(new Entry(path, Metadata.FromNativePayload(metadataPayload)));
            }

            return results;
        }
        finally
        {
            NativeMethods.entry_list_free(ptr);
        }
    }
}
