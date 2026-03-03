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
    internal static partial OpenDALOperatorResult operator_construct(
        string scheme,
        IntPtr options);

    [LibraryImport(__DllName, EntryPoint = "constructor_option_build", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOptionsResult constructor_option_build(
        [In] string[] keys,
        [In] string[] values,
        nuint len);

    [LibraryImport(__DllName, EntryPoint = "constructor_option_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void constructor_option_free(IntPtr options);

    [LibraryImport(__DllName, EntryPoint = "operator_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void operator_free(IntPtr op);

    [LibraryImport(__DllName, EntryPoint = "operator_info_get")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOperatorInfoResult operator_info_get(Operator op);

    [LibraryImport(__DllName, EntryPoint = "operator_info_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void operator_info_free(IntPtr info);

    #endregion

    #region Option Builders

    #region ReadOption

    [LibraryImport(__DllName, EntryPoint = "read_option_build", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOptionsResult read_option_build(
        [In] string[] keys,
        [In] string[] values,
        nuint len);

    [LibraryImport(__DllName, EntryPoint = "read_option_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void read_option_free(IntPtr options);

    #endregion

    #region WriteOption

    [LibraryImport(__DllName, EntryPoint = "write_option_build", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOptionsResult write_option_build(
        [In] string[] keys,
        [In] string[] values,
        nuint len);

    [LibraryImport(__DllName, EntryPoint = "write_option_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void write_option_free(IntPtr options);

    #endregion

    #region StatOption

    [LibraryImport(__DllName, EntryPoint = "stat_option_build", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOptionsResult stat_option_build(
        [In] string[] keys,
        [In] string[] values,
        nuint len);

    [LibraryImport(__DllName, EntryPoint = "stat_option_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void stat_option_free(IntPtr options);

    #endregion

    #region ListOption

    [LibraryImport(__DllName, EntryPoint = "list_option_build", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOptionsResult list_option_build(
        [In] string[] keys,
        [In] string[] values,
        nuint len);

    [LibraryImport(__DllName, EntryPoint = "list_option_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void list_option_free(IntPtr options);

    #endregion

    #endregion

    #region Layer

    [LibraryImport(__DllName, EntryPoint = "operator_layer_retry")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOperatorResult operator_layer_retry(
        Operator op,
        [MarshalAs(UnmanagedType.I1)] bool jitter,
        float factor,
        ulong minDelayNanos,
        ulong maxDelayNanos,
        nuint maxTimes);

    [LibraryImport(__DllName, EntryPoint = "operator_layer_concurrent_limit")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOperatorResult operator_layer_concurrent_limit(
        Operator op,
        nuint permits);

    [LibraryImport(__DllName, EntryPoint = "operator_layer_timeout")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOperatorResult operator_layer_timeout(
        Operator op,
        ulong timeoutNanos,
        ulong ioTimeoutNanos);

    #endregion

    #region IO Operations

    #region Write

    [LibraryImport(__DllName, EntryPoint = "operator_write_with_options", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALResult operator_write_with_options(
        Operator op,
        IntPtr executor,
        string path,
        [In] byte[] data,
        nuint len,
        IntPtr options);

    [LibraryImport(__DllName, EntryPoint = "operator_write_with_options_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_write_with_options_async(
        Operator op,
        IntPtr executor,
        string path,
        [In] byte[] data,
        nuint len,
        IntPtr options,
        delegate* unmanaged[Cdecl]<long, OpenDALResult, void> callback,
        long context);

    #endregion

    #region Read

    [LibraryImport(__DllName, EntryPoint = "operator_read_with_options", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALReadResult operator_read_with_options(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr options);

    [LibraryImport(__DllName, EntryPoint = "operator_read_with_options_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_read_with_options_async(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr options,
        delegate* unmanaged[Cdecl]<long, OpenDALReadResult, void> callback,
        long context);

    #endregion

    #region Stat

    [LibraryImport(__DllName, EntryPoint = "operator_stat_with_options", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALMetadataResult operator_stat_with_options(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr options);

    [LibraryImport(__DllName, EntryPoint = "operator_stat_with_options_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_stat_with_options_async(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr options,
        delegate* unmanaged[Cdecl]<long, OpenDALMetadataResult, void> callback,
        long context);

    #endregion

    #region List

    [LibraryImport(__DllName, EntryPoint = "operator_list_with_options", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALEntryListResult operator_list_with_options(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr options);

    [LibraryImport(__DllName, EntryPoint = "operator_list_with_options_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_list_with_options_async(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr options,
        delegate* unmanaged[Cdecl]<long, OpenDALEntryListResult, void> callback,
        long context)
        ;

    #endregion

    #endregion

    #region Executor

    [LibraryImport(__DllName, EntryPoint = "executor_create")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALExecutorResult executor_create(nuint threads);

    [LibraryImport(__DllName, EntryPoint = "executor_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void executor_free(IntPtr executor);

    #endregion

    #region Result Release

    [LibraryImport(__DllName, EntryPoint = "opendal_result_release")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void opendal_result_release(OpenDALResult result);

    [LibraryImport(__DllName, EntryPoint = "opendal_operator_result_release")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void opendal_operator_result_release(OpenDALOperatorResult result);

    [LibraryImport(__DllName, EntryPoint = "opendal_options_result_release")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void opendal_options_result_release(OpenDALOptionsResult result);

    [LibraryImport(__DllName, EntryPoint = "opendal_executor_result_release")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void opendal_executor_result_release(OpenDALExecutorResult result);

    [LibraryImport(__DllName, EntryPoint = "opendal_operator_info_result_release")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void opendal_operator_info_result_release(OpenDALOperatorInfoResult result);

    [LibraryImport(__DllName, EntryPoint = "opendal_metadata_result_release")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void opendal_metadata_result_release(OpenDALMetadataResult result);

    [LibraryImport(__DllName, EntryPoint = "opendal_entry_list_result_release")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void opendal_entry_list_result_release(OpenDALEntryListResult result);

    [LibraryImport(__DllName, EntryPoint = "opendal_read_result_release")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void opendal_read_result_release(OpenDALReadResult result);

    #endregion

}