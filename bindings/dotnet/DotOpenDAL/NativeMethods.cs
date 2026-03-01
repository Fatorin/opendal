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
using System.Runtime.CompilerServices;

namespace DotOpenDAL;

internal partial class NativeMethods
{
    const string __DllName = "opendal_dotnet";

    #region Operator Lifecycle

    [LibraryImport(__DllName, EntryPoint = "operator_construct", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALPointerResult operator_construct(
        string scheme,
        IntPtr* keys,
        IntPtr* values,
        nuint len);

    [LibraryImport(__DllName, EntryPoint = "operator_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void operator_free(IntPtr op);

    [LibraryImport(__DllName, EntryPoint = "operator_info_get")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALPointerResult operator_info_get(Operator op);

    [LibraryImport(__DllName, EntryPoint = "operator_info_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void operator_info_free(IntPtr info);

    #endregion

    #region IO Operations

    #region Write

    [LibraryImport(__DllName, EntryPoint = "operator_write", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_write(Operator op, IntPtr executor, string path, byte* data, nuint len);

    [LibraryImport(__DllName, EntryPoint = "operator_write_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_write_async(
        Operator op,
        IntPtr executor,
        string path,
        byte* data,
        nuint len,
        delegate* unmanaged[Cdecl]<IntPtr, OpenDALResult, void> callback,
        IntPtr context);

    [LibraryImport(__DllName, EntryPoint = "operator_write_with_options", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_write_with_options(
        Operator op,
        IntPtr executor,
        string path,
        byte* data,
        nuint len,
        IntPtr* optionKeys,
        IntPtr* optionValues,
        nuint optionLen);

    [LibraryImport(__DllName, EntryPoint = "operator_write_with_options_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_write_with_options_async(
        Operator op,
        IntPtr executor,
        string path,
        byte* data,
        nuint len,
        IntPtr* optionKeys,
        IntPtr* optionValues,
        nuint optionLen,
        delegate* unmanaged[Cdecl]<IntPtr, OpenDALResult, void> callback,
        IntPtr context);

    #endregion

    #region Read

    [LibraryImport(__DllName, EntryPoint = "operator_read", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALByteBufferResult operator_read(Operator op, IntPtr executor, string path);

    [LibraryImport(__DllName, EntryPoint = "operator_read_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_read_async(
        Operator op,
        IntPtr executor,
        string path,
        delegate* unmanaged[Cdecl]<IntPtr, OpenDALByteBufferResult, void> callback,
        IntPtr context);

    [LibraryImport(__DllName, EntryPoint = "operator_read_with_options", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALByteBufferResult operator_read_with_options(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr* optionKeys,
        IntPtr* optionValues,
        nuint optionLen);

    [LibraryImport(__DllName, EntryPoint = "operator_read_with_options_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_read_with_options_async(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr* optionKeys,
        IntPtr* optionValues,
        nuint optionLen,
        delegate* unmanaged[Cdecl]<IntPtr, OpenDALByteBufferResult, void> callback,
        IntPtr context);

    #endregion

    #region Stat

    [LibraryImport(__DllName, EntryPoint = "operator_stat_with_options", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALPointerResult operator_stat_with_options(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr* optionKeys,
        IntPtr* optionValues,
        nuint optionLen);

    [LibraryImport(__DllName, EntryPoint = "operator_stat_with_options_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_stat_with_options_async(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr* optionKeys,
        IntPtr* optionValues,
        nuint optionLen,
        delegate* unmanaged[Cdecl]<IntPtr, OpenDALPointerResult, void> callback,
        IntPtr context);

    #endregion

    #region List

    [LibraryImport(__DllName, EntryPoint = "operator_list_with_options", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALPointerResult operator_list_with_options(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr* optionKeys,
        IntPtr* optionValues,
        nuint optionLen);

    [LibraryImport(__DllName, EntryPoint = "operator_list_with_options_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_list_with_options_async(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr* optionKeys,
        IntPtr* optionValues,
        nuint optionLen,
        delegate* unmanaged[Cdecl]<IntPtr, OpenDALPointerResult, void> callback,
        IntPtr context);

    #endregion

    #endregion

    #region Executor

    [LibraryImport(__DllName, EntryPoint = "executor_create")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALPointerResult executor_create(nuint threads);

    [LibraryImport(__DllName, EntryPoint = "executor_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void executor_free(IntPtr executor);

    #endregion

    #region Native Payload Release

    [LibraryImport(__DllName, EntryPoint = "metadata_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void metadata_free(IntPtr metadata);

    [LibraryImport(__DllName, EntryPoint = "entry_list_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void entry_list_free(IntPtr entryList);

    [LibraryImport(__DllName, EntryPoint = "buffer_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void buffer_free(IntPtr data, nuint len, nuint capacity);

    #endregion

    #region Native String Utilities

    [LibraryImport(__DllName, EntryPoint = "string_ptr_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void string_ptr_free(IntPtr message);

    #endregion
}