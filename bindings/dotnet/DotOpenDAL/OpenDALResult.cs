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
public struct OpenDALResult : IAsyncCallbackResult<bool>
{
    /// <summary>
    /// Error details for the operation.
    /// </summary>
    public OpenDALError Error;

    public readonly void Release()
    {
        NativeMethods.opendal_result_release(this);
    }

    public readonly OpenDALError GetError()
    {
        return Error;
    }

    public readonly bool ToValue()
    {
        return Error.IsError;
    }

}

[StructLayout(LayoutKind.Sequential)]
/// <summary>
/// Result wrapper for operations that return an operator pointer payload.
/// </summary>
internal struct OpenDALOperatorResult
{
    /// <summary>
    /// Native pointer payload on success.
    /// </summary>
    public IntPtr Ptr;

    /// <summary>
    /// Error details for the operation.
    /// </summary>
    public OpenDALError Error;

    public readonly void Release()
    {
        NativeMethods.opendal_operator_result_release(this);
    }
}

[StructLayout(LayoutKind.Sequential)]
/// <summary>
/// Result wrapper for operations that return an options pointer payload.
/// </summary>
internal struct OpenDALOptionsResult
{
    public IntPtr Ptr;

    public OpenDALError Error;

    public readonly void Release()
    {
        NativeMethods.opendal_options_result_release(this);
    }
}

[StructLayout(LayoutKind.Sequential)]
/// <summary>
/// Result wrapper for operations that return an executor pointer payload.
/// </summary>
internal struct OpenDALExecutorResult
{
    public IntPtr Ptr;

    public OpenDALError Error;

    public readonly void Release()
    {
        NativeMethods.opendal_executor_result_release(this);
    }
}

[StructLayout(LayoutKind.Sequential)]
/// <summary>
/// Result wrapper for operations that return an operator info payload.
/// </summary>
internal struct OpenDALOperatorInfoResult
    : IAsyncCallbackResult<OperatorInfo>
{
    public IntPtr Ptr;

    public OpenDALError Error;

    public readonly void Release()
    {
        NativeMethods.opendal_operator_info_result_release(this);
    }

    public readonly OpenDALError GetError()
    {
        return Error;
    }

    public readonly OperatorInfo ToValue()
    {
        return OperatorInfo.FromNativePointer(Ptr);
    }
}

[StructLayout(LayoutKind.Sequential)]
/// <summary>
/// Result wrapper for operations that return a metadata payload.
/// </summary>
internal struct OpenDALMetadataResult : IAsyncCallbackResult<Metadata>
{
    public IntPtr Ptr;

    public OpenDALError Error;

    public readonly void Release()
    {
        NativeMethods.opendal_metadata_result_release(this);
    }

    public readonly OpenDALError GetError()
    {
        return Error;
    }

    public readonly Metadata ToValue()
    {
        return Metadata.FromNativePointer(Ptr);
    }
}

[StructLayout(LayoutKind.Sequential)]
/// <summary>
/// Result wrapper for operations that return an entry list payload.
/// </summary>
internal struct OpenDALEntryListResult : IAsyncCallbackResult<IReadOnlyList<Entry>>
{
    public IntPtr Ptr;

    public OpenDALError Error;

    public readonly void Release()
    {
        NativeMethods.opendal_entry_list_result_release(this);
    }

    public readonly OpenDALError GetError()
    {
        return Error;
    }

    public readonly IReadOnlyList<Entry> ToValue()
    {
        return Entry.FromNativePointer(Ptr);
    }
}

[StructLayout(LayoutKind.Sequential)]
/// <summary>
/// Result wrapper for operations that return a byte buffer payload.
/// </summary>
internal struct OpenDALReadResult : IAsyncCallbackResult<byte[]>
{
    /// <summary>
    /// Byte buffer payload on success.
    /// </summary>
    public ByteBuffer Buffer;

    /// <summary>
    /// Error details for the operation.
    /// </summary>
    public OpenDALError Error;

    public readonly void Release()
    {
        NativeMethods.opendal_read_result_release(this);
    }

    public readonly OpenDALError GetError()
    {
        return Error;
    }

    public readonly byte[] ToValue()
    {
        return Buffer.ToManagedBytes();
    }
}