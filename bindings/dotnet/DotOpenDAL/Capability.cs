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
/// Capability is used to describe what operations are supported by current operator.
/// </summary>
/// <remarks>
/// This model maps from native capability payload returned by OpenDAL.
/// For write multi size fields, nullable values are represented by a native sentinel value.
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
public struct Capability
{
    [MarshalAs(UnmanagedType.U1)] internal byte stat;
    [MarshalAs(UnmanagedType.U1)] internal byte statWithIfMatch;
    [MarshalAs(UnmanagedType.U1)] internal byte statWithIfNoneMatch;
    [MarshalAs(UnmanagedType.U1)] internal byte statWithIfModifiedSince;
    [MarshalAs(UnmanagedType.U1)] internal byte statWithIfUnmodifiedSince;
    [MarshalAs(UnmanagedType.U1)] internal byte statWithOverrideCacheControl;
    [MarshalAs(UnmanagedType.U1)] internal byte statWithOverrideContentDisposition;
    [MarshalAs(UnmanagedType.U1)] internal byte statWithOverrideContentType;
    [MarshalAs(UnmanagedType.U1)] internal byte statWithVersion;

    [MarshalAs(UnmanagedType.U1)] internal byte read;
    [MarshalAs(UnmanagedType.U1)] internal byte readWithIfMatch;
    [MarshalAs(UnmanagedType.U1)] internal byte readWithIfNoneMatch;
    [MarshalAs(UnmanagedType.U1)] internal byte readWithIfModifiedSince;
    [MarshalAs(UnmanagedType.U1)] internal byte readWithIfUnmodifiedSince;
    [MarshalAs(UnmanagedType.U1)] internal byte readWithOverrideCacheControl;
    [MarshalAs(UnmanagedType.U1)] internal byte readWithOverrideContentDisposition;
    [MarshalAs(UnmanagedType.U1)] internal byte readWithOverrideContentType;
    [MarshalAs(UnmanagedType.U1)] internal byte readWithVersion;

    [MarshalAs(UnmanagedType.U1)] internal byte write;
    [MarshalAs(UnmanagedType.U1)] internal byte writeCanMulti;
    [MarshalAs(UnmanagedType.U1)] internal byte writeCanEmpty;
    [MarshalAs(UnmanagedType.U1)] internal byte writeCanAppend;
    [MarshalAs(UnmanagedType.U1)] internal byte writeWithContentType;
    [MarshalAs(UnmanagedType.U1)] internal byte writeWithContentDisposition;
    [MarshalAs(UnmanagedType.U1)] internal byte writeWithContentEncoding;
    [MarshalAs(UnmanagedType.U1)] internal byte writeWithCacheControl;
    [MarshalAs(UnmanagedType.U1)] internal byte writeWithIfMatch;
    [MarshalAs(UnmanagedType.U1)] internal byte writeWithIfNoneMatch;
    [MarshalAs(UnmanagedType.U1)] internal byte writeWithIfNotExists;
    [MarshalAs(UnmanagedType.U1)] internal byte writeWithUserMetadata;

    internal nuint writeMultiMaxSize;
    internal nuint writeMultiMinSize;
    internal nuint writeTotalMaxSize;

    [MarshalAs(UnmanagedType.U1)] internal byte createDir;
    [MarshalAs(UnmanagedType.U1)] internal byte delete;
    [MarshalAs(UnmanagedType.U1)] internal byte deleteWithVersion;
    [MarshalAs(UnmanagedType.U1)] internal byte deleteWithRecursive;
    internal nuint deleteMaxSize;

    [MarshalAs(UnmanagedType.U1)] internal byte copy;
    [MarshalAs(UnmanagedType.U1)] internal byte copyWithIfNotExists;
    [MarshalAs(UnmanagedType.U1)] internal byte rename;

    [MarshalAs(UnmanagedType.U1)] internal byte list;
    [MarshalAs(UnmanagedType.U1)] internal byte listWithLimit;
    [MarshalAs(UnmanagedType.U1)] internal byte listWithStartAfter;
    [MarshalAs(UnmanagedType.U1)] internal byte listWithRecursive;
    [MarshalAs(UnmanagedType.U1)] internal byte listWithVersions;
    [MarshalAs(UnmanagedType.U1)] internal byte listWithDeleted;

    [MarshalAs(UnmanagedType.U1)] internal byte presign;
    [MarshalAs(UnmanagedType.U1)] internal byte presignRead;
    [MarshalAs(UnmanagedType.U1)] internal byte presignStat;
    [MarshalAs(UnmanagedType.U1)] internal byte presignWrite;
    [MarshalAs(UnmanagedType.U1)] internal byte presignDelete;

    [MarshalAs(UnmanagedType.U1)] internal byte shared;

    /// <summary>
    /// If operator supports stat.
    /// </summary>
    public bool Stat => stat != 0;

    /// <summary>
    /// If operator supports stat with if match.
    /// </summary>
    public bool StatWithIfMatch => statWithIfMatch != 0;

    /// <summary>
    /// If operator supports stat with if none match.
    /// </summary>
    public bool StatWithIfNoneMatch => statWithIfNoneMatch != 0;

    /// <summary>
    /// If operator supports stat with if modified since.
    /// </summary>
    public bool StatWithIfModifiedSince => statWithIfModifiedSince != 0;

    /// <summary>
    /// If operator supports stat with if unmodified since.
    /// </summary>
    public bool StatWithIfUnmodifiedSince => statWithIfUnmodifiedSince != 0;

    /// <summary>
    /// If operator supports stat with override cache control.
    /// </summary>
    public bool StatWithOverrideCacheControl => statWithOverrideCacheControl != 0;

    /// <summary>
    /// If operator supports stat with override content disposition.
    /// </summary>
    public bool StatWithOverrideContentDisposition => statWithOverrideContentDisposition != 0;

    /// <summary>
    /// If operator supports stat with override content type.
    /// </summary>
    public bool StatWithOverrideContentType => statWithOverrideContentType != 0;

    /// <summary>
    /// If operator supports stat with version.
    /// </summary>
    public bool StatWithVersion => statWithVersion != 0;

    /// <summary>
    /// If operator supports read.
    /// </summary>
    public bool Read => read != 0;

    /// <summary>
    /// If operator supports read with if match.
    /// </summary>
    public bool ReadWithIfMatch => readWithIfMatch != 0;

    /// <summary>
    /// If operator supports read with if none match.
    /// </summary>
    public bool ReadWithIfNoneMatch => readWithIfNoneMatch != 0;

    /// <summary>
    /// If operator supports read with if modified since.
    /// </summary>
    public bool ReadWithIfModifiedSince => readWithIfModifiedSince != 0;

    /// <summary>
    /// If operator supports read with if unmodified since.
    /// </summary>
    public bool ReadWithIfUnmodifiedSince => readWithIfUnmodifiedSince != 0;

    /// <summary>
    /// If operator supports read with override cache control.
    /// </summary>
    public bool ReadWithOverrideCacheControl => readWithOverrideCacheControl != 0;

    /// <summary>
    /// If operator supports read with override content disposition.
    /// </summary>
    public bool ReadWithOverrideContentDisposition => readWithOverrideContentDisposition != 0;

    /// <summary>
    /// If operator supports read with override content type.
    /// </summary>
    public bool ReadWithOverrideContentType => readWithOverrideContentType != 0;

    /// <summary>
    /// If operator supports read with version.
    /// </summary>
    public bool ReadWithVersion => readWithVersion != 0;

    /// <summary>
    /// If operator supports write.
    /// </summary>
    public bool Write => write != 0;

    /// <summary>
    /// If operator supports write can be called in multi times.
    /// </summary>
    public bool WriteCanMulti => writeCanMulti != 0;

    /// <summary>
    /// If operator supports write with empty content.
    /// </summary>
    public bool WriteCanEmpty => writeCanEmpty != 0;

    /// <summary>
    /// If operator supports write by append.
    /// </summary>
    public bool WriteCanAppend => writeCanAppend != 0;

    /// <summary>
    /// If operator supports write with content type.
    /// </summary>
    public bool WriteWithContentType => writeWithContentType != 0;

    /// <summary>
    /// If operator supports write with content disposition.
    /// </summary>
    public bool WriteWithContentDisposition => writeWithContentDisposition != 0;

    /// <summary>
    /// If operator supports write with content encoding.
    /// </summary>
    public bool WriteWithContentEncoding => writeWithContentEncoding != 0;

    /// <summary>
    /// If operator supports write with cache control.
    /// </summary>
    public bool WriteWithCacheControl => writeWithCacheControl != 0;

    /// <summary>
    /// If operator supports write with if match.
    /// </summary>
    public bool WriteWithIfMatch => writeWithIfMatch != 0;

    /// <summary>
    /// If operator supports write with if none match.
    /// </summary>
    public bool WriteWithIfNoneMatch => writeWithIfNoneMatch != 0;

    /// <summary>
    /// If operator supports write with if not exists.
    /// </summary>
    public bool WriteWithIfNotExists => writeWithIfNotExists != 0;

    /// <summary>
    /// If operator supports write with user metadata.
    /// </summary>
    public bool WriteWithUserMetadata => writeWithUserMetadata != 0;

    /// <summary>
    /// write_multi_max_size is the max size that services support in write_multi.
    /// </summary>
    public ulong? WriteMultiMaxSize => writeMultiMaxSize == ulong.MinValue ? null : writeMultiMaxSize;

    /// <summary>
    /// write_multi_min_size is the min size that services support in write_multi.
    /// </summary>
    public ulong? WriteMultiMinSize => writeMultiMinSize == ulong.MinValue ? null : writeMultiMinSize;

    /// <summary>
    /// write_total_max_size is the max total size that services support in write.
    /// </summary>
    public ulong? WriteTotalMaxSize => writeTotalMaxSize == ulong.MinValue ? null : writeTotalMaxSize;

    /// <summary>
    /// If operator supports create dir.
    /// </summary>
    public bool CreateDir => createDir != 0;

    /// <summary>
    /// If operator supports delete.
    /// </summary>
    public bool Delete => delete != 0;

    /// <summary>
    /// If operator supports delete with version.
    /// </summary>
    public bool DeleteWithVersion => deleteWithVersion != 0;

    /// <summary>
    /// If operator supports delete with recursive.
    /// </summary>
    public bool DeleteWithRecursive => deleteWithRecursive != 0;

    /// <summary>
    /// delete_max_size is the max size that services support in delete.
    /// </summary>
    public ulong? DeleteMaxSize => deleteMaxSize == ulong.MinValue ? null : deleteMaxSize;

    /// <summary>
    /// If operator supports copy.
    /// </summary>
    public bool Copy => copy != 0;

    /// <summary>
    /// If operator supports copy with if not exists.
    /// </summary>
    public bool CopyWithIfNotExists => copyWithIfNotExists != 0;

    /// <summary>
    /// If operator supports rename.
    /// </summary>
    public bool Rename => rename != 0;

    /// <summary>
    /// If operator supports list.
    /// </summary>
    public bool List => list != 0;

    /// <summary>
    /// If backend supports list with limit.
    /// </summary>
    public bool ListWithLimit => listWithLimit != 0;

    /// <summary>
    /// If backend supports list with start after.
    /// </summary>
    public bool ListWithStartAfter => listWithStartAfter != 0;

    /// <summary>
    /// If backend supports list with recursive.
    /// </summary>
    public bool ListWithRecursive => listWithRecursive != 0;

    /// <summary>
    /// If backend supports list with versions.
    /// </summary>
    public bool ListWithVersions => listWithVersions != 0;

    /// <summary>
    /// If backend supports list with deleted.
    /// </summary>
    public bool ListWithDeleted => listWithDeleted != 0;

    /// <summary>
    /// If operator supports presign.
    /// </summary>
    public bool Presign => presign != 0;

    /// <summary>
    /// If operator supports presign read.
    /// </summary>
    public bool PresignRead => presignRead != 0;

    /// <summary>
    /// If operator supports presign stat.
    /// </summary>
    public bool PresignStat => presignStat != 0;

    /// <summary>
    /// If operator supports presign write.
    /// </summary>
    public bool PresignWrite => presignWrite != 0;

    /// <summary>
    /// If operator supports presign delete.
    /// </summary>
    public bool PresignDelete => presignDelete != 0;

    /// <summary>
    /// If operator supports shared.
    /// </summary>
    public bool Shared => shared != 0;
}