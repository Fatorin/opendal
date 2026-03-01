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

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using DotOpenDAL.ServiceConfig.Abstractions;

namespace DotOpenDAL;

/// <summary>
/// Managed wrapper over an OpenDAL native operator handle.
/// </summary>
public partial class Operator : SafeHandle
{
    private unsafe delegate TResult NativeOptionsInvoker<TResult>(IntPtr* keys, IntPtr* values, nuint len);

    /// <summary>
    /// Gets metadata of this operator.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The operator has been disposed.</exception>
    /// <exception cref="OpenDALException">Native operator info retrieval fails.</exception>
    public OperatorInfo Info
    {
        get
        {
            ObjectDisposedException.ThrowIf(IsInvalid, this);
            var result = NativeMethods.operator_info_get(this);
            if (result.Error.IsError)
            {
                throw new OpenDALException(result.Error);
            }

            if (result.Ptr == IntPtr.Zero)
            {
                throw new InvalidOperationException("operator_info_get returned null pointer");
            }

            var payload = Marshal.PtrToStructure<OpenDALOperatorInfo>(result.Ptr);

            try
            {
                return new OperatorInfo(
                    Utilities.ReadUtf8(payload.Scheme),
                    Utilities.ReadUtf8(payload.Root),
                    Utilities.ReadUtf8(payload.Name),
                    payload.FullCapability,
                    payload.NativeCapability);
            }
            finally
            {
                NativeMethods.operator_info_free(result.Ptr);
            }
        }
    }

    /// <summary>
    /// Gets the underlying native operator pointer.
    /// </summary>
    public IntPtr Op => DangerousGetHandle();

    /// <summary>
    /// Gets whether the native handle is invalid.
    /// </summary>
    public override bool IsInvalid => handle == IntPtr.Zero;

    /// <summary>
    /// Creates an operator for the specified backend scheme and options.
    /// </summary>
    /// <param name="scheme">Backend scheme, such as <c>fs</c> or <c>memory</c>.</param>
    /// <param name="options">Backend-specific key/value options.</param>
    /// <exception cref="ArgumentException"><paramref name="scheme"/> is null, empty, or whitespace.</exception>
    /// <exception cref="OpenDALException">Native operator construction fails.</exception>
    public unsafe Operator(string scheme, IReadOnlyDictionary<string, string>? options = null) : base(IntPtr.Zero, true)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheme);

        var optionCount = options?.Count ?? 0;
        var keyPointers = optionCount > 0 ? new IntPtr[optionCount] : Array.Empty<IntPtr>();
        var valuePointers = optionCount > 0 ? new IntPtr[optionCount] : Array.Empty<IntPtr>();

        OpenDALIntPtrResult result;
        try
        {
            if (optionCount > 0)
            {
                var index = 0;
                foreach (var option in options!)
                {
                    keyPointers[index] = Marshal.StringToCoTaskMemUTF8(option.Key);
                    valuePointers[index] = Marshal.StringToCoTaskMemUTF8(option.Value);
                    index++;
                }

                fixed (IntPtr* keyPtr = keyPointers)
                fixed (IntPtr* valuePtr = valuePointers)
                {
                    result = NativeMethods.operator_construct(scheme, keyPtr, valuePtr, (nuint)optionCount);
                }
            }
            else
            {
                result = NativeMethods.operator_construct(scheme, null, null, 0);
            }
        }
        finally
        {
            for (var index = 0; index < optionCount; index++)
            {
                if (keyPointers[index] != IntPtr.Zero)
                {
                    Marshal.FreeCoTaskMem(keyPointers[index]);
                }

                if (valuePointers[index] != IntPtr.Zero)
                {
                    Marshal.FreeCoTaskMem(valuePointers[index]);
                }
            }
        }

        if (result.Ptr == IntPtr.Zero)
        {
            throw new OpenDALException(result.Error);
        }

        SetHandle(result.Ptr);
    }

    /// <summary>
    /// Creates an operator from a typed service configuration.
    /// </summary>
    /// <param name="config">Typed service configuration.</param>
    /// <exception cref="ArgumentNullException"><paramref name="config"/> is null.</exception>
    /// <exception cref="OpenDALException">Native operator construction fails.</exception>
    public Operator(IServiceConfig config) : this(
        config?.Scheme ?? throw new ArgumentNullException(nameof(config)),
        config.ToOptions())
    {
    }

    /// <summary>
    /// Writes the specified content to a path.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <exception cref="ObjectDisposedException">The operator has been disposed.</exception>
    /// <exception cref="OpenDALException">Native write fails.</exception>
    public void Write(string path, byte[] content)
    {
        Write(path, content, executor: null);
    }

    /// <summary>
    /// Writes the specified content to a path with write options.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <param name="options">Additional write options.</param>
    public void Write(string path, byte[] content, WriteOptions options)
    {
        Write(path, content, options, executor: null);
    }

    /// <summary>
    /// Writes the specified content to a path using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <exception cref="ObjectDisposedException">The operator or executor has been disposed.</exception>
    /// <exception cref="OpenDALException">Native write fails.</exception>
    public void Write(string path, byte[] content, Executor? executor)
    {
        ArgumentNullException.ThrowIfNull(content);
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);

        unsafe
        {
            fixed (byte* ptr = content)
            {
                var result = NativeMethods.operator_write(this, executorHandle, path, ptr, (nuint)content.Length);
                if (result.Error.IsError)
                {
                    throw new OpenDALException(result.Error);
                }
            }
        }
    }

    /// <summary>
    /// Writes the specified content to a path with write options using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <param name="options">Additional write options.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    public void Write(string path, byte[] content, WriteOptions options, Executor? executor)
    {
        ArgumentNullException.ThrowIfNull(content);
        ArgumentNullException.ThrowIfNull(options);
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);
        var nativeOptions = options.ToNativeOptions();
        var optionCount = nativeOptions.Count;
        var keyPointers = optionCount > 0 ? new IntPtr[optionCount] : Array.Empty<IntPtr>();
        var valuePointers = optionCount > 0 ? new IntPtr[optionCount] : Array.Empty<IntPtr>();

        OpenDALResult result;
        try
        {
            if (optionCount > 0)
            {
                var optionIndex = 0;
                foreach (var option in nativeOptions)
                {
                    keyPointers[optionIndex] = Marshal.StringToCoTaskMemUTF8(option.Key);
                    valuePointers[optionIndex] = Marshal.StringToCoTaskMemUTF8(option.Value);
                    optionIndex++;
                }

                unsafe
                {
                    fixed (byte* ptr = content)
                    fixed (IntPtr* keyPtr = keyPointers)
                    fixed (IntPtr* valuePtr = valuePointers)
                    {
                        result = NativeMethods.operator_write_with_options(
                            this,
                            executorHandle,
                            path,
                            ptr,
                            (nuint)content.Length,
                            keyPtr,
                            valuePtr,
                            (nuint)optionCount);
                    }
                }
            }
            else
            {
                unsafe
                {
                    fixed (byte* ptr = content)
                    {
                        result = NativeMethods.operator_write_with_options(
                            this,
                            executorHandle,
                            path,
                            ptr,
                            (nuint)content.Length,
                            null,
                            null,
                            0);
                    }
                }
            }
        }
        finally
        {
            for (var optionIndex = 0; optionIndex < optionCount; optionIndex++)
            {
                if (keyPointers[optionIndex] != IntPtr.Zero)
                {
                    Marshal.FreeCoTaskMem(keyPointers[optionIndex]);
                }

                if (valuePointers[optionIndex] != IntPtr.Zero)
                {
                    Marshal.FreeCoTaskMem(valuePointers[optionIndex]);
                }
            }
        }

        if (result.Error.IsError)
        {
            throw new OpenDALException(result.Error);
        }
    }

    /// <summary>
    /// Writes the specified content to a path asynchronously.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    /// <exception cref="ObjectDisposedException">The operator has been disposed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
    /// <exception cref="OpenDALException">Native write submission fails immediately.</exception>
    public unsafe Task WriteAsync(string path, byte[] content, CancellationToken cancellationToken = default)
    {
        return WriteAsync(path, content, executor: null, cancellationToken);
    }

    /// <summary>
    /// Writes the specified content to a path asynchronously with write options.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <param name="options">Additional write options.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    public unsafe Task WriteAsync(
        string path,
        byte[] content,
        WriteOptions options,
        CancellationToken cancellationToken = default)
    {
        return WriteAsync(path, content, options, executor: null, cancellationToken);
    }

    /// <summary>
    /// Writes the specified content to a path asynchronously using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    /// <exception cref="ObjectDisposedException">The operator or executor has been disposed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
    /// <exception cref="OpenDALException">Native write submission fails immediately.</exception>
    public unsafe Task WriteAsync(
        string path,
        byte[] content,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        cancellationToken.ThrowIfCancellationRequested();
        var executorHandle = GetExecutorHandle(executor);

        var state = new WriteAsyncState();
        var context = AsyncStateRegistry.Register(state);

        OpenDALResult result;
        fixed (byte* ptr = content)
        {
            result = NativeMethods.operator_write_async(
                this,
                executorHandle,
                path,
                ptr,
                (nuint)content.Length,
                &OnWriteCompleted,
                new IntPtr(context));
        }

        if (result.Error.IsError)
        {
            AsyncStateRegistry.Unregister(context);
            throw new OpenDALException(result.Error);
        }

        if (cancellationToken.CanBeCanceled)
        {
            state.CancellationRegistration = cancellationToken.Register(static value =>
            {
                var current = (WriteAsyncState)value!;
                current.Completion.TrySetCanceled();
            }, state);
        }

        return state.Completion.Task;
    }

    /// <summary>
    /// Writes the specified content to a path asynchronously with write options using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <param name="options">Additional write options.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    public unsafe Task WriteAsync(
        string path,
        byte[] content,
        WriteOptions options,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);
        ArgumentNullException.ThrowIfNull(options);
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        cancellationToken.ThrowIfCancellationRequested();
        var executorHandle = GetExecutorHandle(executor);
        var nativeOptions = options.ToNativeOptions();
        var optionCount = nativeOptions.Count;
        var keyPointers = optionCount > 0 ? new IntPtr[optionCount] : Array.Empty<IntPtr>();
        var valuePointers = optionCount > 0 ? new IntPtr[optionCount] : Array.Empty<IntPtr>();

        var state = new WriteAsyncState();
        var context = AsyncStateRegistry.Register(state);

        OpenDALResult result;
        try
        {
            if (optionCount > 0)
            {
                var optionIndex = 0;
                foreach (var option in nativeOptions)
                {
                    keyPointers[optionIndex] = Marshal.StringToCoTaskMemUTF8(option.Key);
                    valuePointers[optionIndex] = Marshal.StringToCoTaskMemUTF8(option.Value);
                    optionIndex++;
                }

                fixed (byte* ptr = content)
                fixed (IntPtr* keyPtr = keyPointers)
                fixed (IntPtr* valuePtr = valuePointers)
                {
                    result = NativeMethods.operator_write_with_options_async(
                        this,
                        executorHandle,
                        path,
                        ptr,
                        (nuint)content.Length,
                        keyPtr,
                        valuePtr,
                        (nuint)optionCount,
                        &OnWriteCompleted,
                        new IntPtr(context));
                }
            }
            else
            {
                fixed (byte* ptr = content)
                {
                    result = NativeMethods.operator_write_with_options_async(
                        this,
                        executorHandle,
                        path,
                        ptr,
                        (nuint)content.Length,
                        null,
                        null,
                        0,
                        &OnWriteCompleted,
                        new IntPtr(context));
                }
            }
        }
        finally
        {
            for (var optionIndex = 0; optionIndex < optionCount; optionIndex++)
            {
                if (keyPointers[optionIndex] != IntPtr.Zero)
                {
                    Marshal.FreeCoTaskMem(keyPointers[optionIndex]);
                }

                if (valuePointers[optionIndex] != IntPtr.Zero)
                {
                    Marshal.FreeCoTaskMem(valuePointers[optionIndex]);
                }
            }
        }

        if (result.Error.IsError)
        {
            AsyncStateRegistry.Unregister(context);
            throw new OpenDALException(result.Error);
        }

        if (cancellationToken.CanBeCanceled)
        {
            state.CancellationRegistration = cancellationToken.Register(static value =>
            {
                var current = (WriteAsyncState)value!;
                current.Completion.TrySetCanceled();
            }, state);
        }

        return state.Completion.Task;
    }

    /// <summary>
    /// Reads all bytes from a path.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <returns>The content bytes.</returns>
    /// <exception cref="ObjectDisposedException">The operator has been disposed.</exception>
    /// <exception cref="OpenDALException">Native read fails.</exception>
    public byte[] Read(string path)
    {
        return Read(path, executor: null);
    }

    /// <summary>
    /// Reads bytes from a path with read options.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <param name="options">Additional read options.</param>
    /// <returns>The content bytes.</returns>
    public byte[] Read(string path, ReadOptions options)
    {
        return Read(path, options, executor: null);
    }

    /// <summary>
    /// Reads all bytes from a path using the provided executor.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <returns>The content bytes.</returns>
    /// <exception cref="ObjectDisposedException">The operator or executor has been disposed.</exception>
    /// <exception cref="OpenDALException">Native read fails.</exception>
    public byte[] Read(string path, Executor? executor)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);

        var result = NativeMethods.operator_read(this, executorHandle, path);
        if (result.Error.IsError)
        {
            throw new OpenDALException(result.Error);
        }

        try
        {
            if (result.Buffer.Data == IntPtr.Zero || result.Buffer.Len == 0)
            {
                return Array.Empty<byte>();
            }

            var size = checked((int)result.Buffer.Len);
            var managed = new byte[size];
            Marshal.Copy(result.Buffer.Data, managed, 0, size);
            return managed;
        }
        finally
        {
            result.Buffer.Release();
        }
    }

    /// <summary>
    /// Reads bytes from a path with read options using the provided executor.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <param name="options">Additional read options.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <returns>The content bytes.</returns>
    public byte[] Read(string path, ReadOptions options, Executor? executor)
    {
        ArgumentNullException.ThrowIfNull(options);
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);
        var nativeOptions = options.ToNativeOptions();

        OpenDALByteBufferResult result;
        unsafe
        {
            result = WithNativeOptions(nativeOptions, (keys, values, len) =>
                NativeMethods.operator_read_with_options(this, executorHandle, path, keys, values, len));
        }
        if (result.Error.IsError)
        {
            throw new OpenDALException(result.Error);
        }

        try
        {
            if (result.Buffer.Data == IntPtr.Zero || result.Buffer.Len == 0)
            {
                return Array.Empty<byte>();
            }

            var size = checked((int)result.Buffer.Len);
            var managed = new byte[size];
            Marshal.Copy(result.Buffer.Data, managed, 0, size);
            return managed;
        }
        finally
        {
            result.Buffer.Release();
        }
    }

    /// <summary>
    /// Reads all bytes from a path asynchronously.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that resolves with the read content.</returns>
    /// <exception cref="ObjectDisposedException">The operator has been disposed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
    /// <exception cref="OpenDALException">Native read submission fails immediately.</exception>
    public unsafe Task<byte[]> ReadAsync(string path, CancellationToken cancellationToken = default)
    {
        return ReadAsync(path, executor: null, cancellationToken);
    }

    /// <summary>
    /// Reads bytes from a path asynchronously with read options.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <param name="options">Additional read options.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that resolves with the read content.</returns>
    public unsafe Task<byte[]> ReadAsync(string path, ReadOptions options, CancellationToken cancellationToken = default)
    {
        return ReadAsync(path, options, executor: null, cancellationToken);
    }

    /// <summary>
    /// Reads all bytes from a path asynchronously using the provided executor.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that resolves with the read content.</returns>
    /// <exception cref="ObjectDisposedException">The operator or executor has been disposed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
    /// <exception cref="OpenDALException">Native read submission fails immediately.</exception>
    public unsafe Task<byte[]> ReadAsync(
        string path,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        cancellationToken.ThrowIfCancellationRequested();
        var executorHandle = GetExecutorHandle(executor);

        var state = new ReadAsyncState();
        var context = AsyncStateRegistry.Register(state);

        var result = NativeMethods.operator_read_async(
            this,
            executorHandle,
            path,
            &OnReadCompleted,
            new IntPtr(context));

        if (result.Error.IsError)
        {
            AsyncStateRegistry.Unregister(context);
            throw new OpenDALException(result.Error);
        }

        if (cancellationToken.CanBeCanceled)
        {
            state.CancellationRegistration = cancellationToken.Register(static value =>
            {
                var current = (ReadAsyncState)value!;
                current.Completion.TrySetCanceled();
            }, state);
        }

        return state.Completion.Task;
    }

    /// <summary>
    /// Reads bytes from a path asynchronously with read options using the provided executor.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <param name="options">Additional read options.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that resolves with the read content.</returns>
    public unsafe Task<byte[]> ReadAsync(
        string path,
        ReadOptions options,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        cancellationToken.ThrowIfCancellationRequested();
        var executorHandle = GetExecutorHandle(executor);
        var nativeOptions = options.ToNativeOptions();

        var state = new ReadAsyncState();
        var context = AsyncStateRegistry.Register(state);

        var result = WithNativeOptions(nativeOptions, (keys, values, len) =>
            NativeMethods.operator_read_with_options_async(
                this,
                executorHandle,
                path,
                keys,
                values,
                len,
                &OnReadCompleted,
                new IntPtr(context)));

        if (result.Error.IsError)
        {
            AsyncStateRegistry.Unregister(context);
            throw new OpenDALException(result.Error);
        }

        if (cancellationToken.CanBeCanceled)
        {
            state.CancellationRegistration = cancellationToken.Register(static value =>
            {
                var current = (ReadAsyncState)value!;
                current.Completion.TrySetCanceled();
            }, state);
        }

        return state.Completion.Task;
    }

    /// <summary>
    /// Gets metadata for the specified path.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="options">Additional stat options.</param>
    /// <returns>Metadata of the target path.</returns>
    public Metadata Stat(string path, StatOptions? options = null)
    {
        return Stat(path, options, executor: null);
    }

    /// <summary>
    /// Gets metadata for the specified path using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="options">Additional stat options.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <returns>Metadata of the target path.</returns>
    public Metadata Stat(string path, StatOptions? options, Executor? executor)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);
        var nativeOptions = options?.ToNativeOptions() ?? new Dictionary<string, string>();

        OpenDALMetadataResult result;
        unsafe
        {
            result = WithNativeOptions(nativeOptions, (keys, values, len) =>
                NativeMethods.operator_stat_with_options(this, executorHandle, path, keys, values, len));
        }
        if (result.Error.IsError)
        {
            throw new OpenDALException(result.Error);
        }

        return FromMetadataPointer(result.Ptr);
    }

    /// <summary>
    /// Gets metadata for the specified path asynchronously.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="options">Additional stat options.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that resolves with metadata.</returns>
    public Task<Metadata> StatAsync(
        string path,
        StatOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        return StatAsync(path, options, executor: null, cancellationToken);
    }

    /// <summary>
    /// Gets metadata for the specified path asynchronously using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="options">Additional stat options.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that resolves with metadata.</returns>
    public unsafe Task<Metadata> StatAsync(
        string path,
        StatOptions? options,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        cancellationToken.ThrowIfCancellationRequested();
        var executorHandle = GetExecutorHandle(executor);
        var nativeOptions = options?.ToNativeOptions() ?? new Dictionary<string, string>();

        var state = new StatAsyncState();
        var context = AsyncStateRegistry.Register(state);

        var result = WithNativeOptions(nativeOptions, (keys, values, len) =>
            NativeMethods.operator_stat_with_options_async(
                this,
                executorHandle,
                path,
                keys,
                values,
                len,
                &OnStatCompleted,
                new IntPtr(context)));

        if (result.Error.IsError)
        {
            AsyncStateRegistry.Unregister(context);
            throw new OpenDALException(result.Error);
        }

        if (cancellationToken.CanBeCanceled)
        {
            state.CancellationRegistration = cancellationToken.Register(static value =>
            {
                var current = (StatAsyncState)value!;
                current.Completion.TrySetCanceled();
            }, state);
        }

        return state.Completion.Task;
    }

    /// <summary>
    /// Releases the native operator handle.
    /// </summary>
    /// <returns><see langword="true"/> after the handle has been released.</returns>
    protected override bool ReleaseHandle()
    {
        NativeMethods.operator_free(handle);
        return true;
    }

    private static IntPtr GetExecutorHandle(Executor? executor)
    {
        if (executor is null)
        {
            return IntPtr.Zero;
        }

        ObjectDisposedException.ThrowIf(executor.IsClosed || executor.IsInvalid, executor);
        return executor.DangerousGetHandle();
    }

    private static unsafe TResult WithNativeOptions<TResult>(
        IReadOnlyDictionary<string, string> options,
        NativeOptionsInvoker<TResult> invoker)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.Count == 0)
        {
            return invoker(null, null, 0);
        }

        var keyPointers = new IntPtr[options.Count];
        var valuePointers = new IntPtr[options.Count];
        var index = 0;

        try
        {
            foreach (var option in options)
            {
                keyPointers[index] = Marshal.StringToCoTaskMemUTF8(option.Key);
                valuePointers[index] = Marshal.StringToCoTaskMemUTF8(option.Value);
                index++;
            }

            fixed (IntPtr* keyPtr = keyPointers)
            fixed (IntPtr* valuePtr = valuePointers)
            {
                return invoker(keyPtr, valuePtr, (nuint)options.Count);
            }
        }
        finally
        {
            for (var pointerIndex = 0; pointerIndex < options.Count; pointerIndex++)
            {
                if (keyPointers[pointerIndex] != IntPtr.Zero)
                {
                    Marshal.FreeCoTaskMem(keyPointers[pointerIndex]);
                }

                if (valuePointers[pointerIndex] != IntPtr.Zero)
                {
                    Marshal.FreeCoTaskMem(valuePointers[pointerIndex]);
                }
            }
        }
    }

    private static Metadata FromMetadataPointer(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
        {
            throw new InvalidOperationException("stat returned null metadata pointer");
        }

        var payload = Marshal.PtrToStructure<OpenDALMetadata>(ptr);

        try
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
        finally
        {
            NativeMethods.metadata_free(ptr);
        }
    }

    /// <summary>
    /// Native callback invoked when an asynchronous write operation finishes.
    /// </summary>
    /// <param name="context">Opaque async state context previously registered by <see cref="AsyncStateRegistry"/>.</param>
    /// <param name="result">Write completion result returned by the native layer.</param>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnWriteCompleted(IntPtr context, OpenDALResult result)
    {
        if (!AsyncStateRegistry.TryTake<WriteAsyncState>(context, out var state))
        {
            result.Error.Release();
            return;
        }

        try
        {
            state.CancellationRegistration.Dispose();

            if (result.Error.IsError)
            {
                state.Completion.TrySetException(new OpenDALException(result.Error));
                return;
            }

            state.Completion.TrySetResult();
        }
        catch (Exception ex)
        {
            state.Completion.TrySetException(ex);
        }
    }

    /// <summary>
    /// Native callback invoked when an asynchronous read operation finishes.
    /// </summary>
    /// <param name="context">Opaque async state context previously registered by <see cref="AsyncStateRegistry"/>.</param>
    /// <param name="result">Read completion result returned by the native layer, including byte buffer payload.</param>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnReadCompleted(IntPtr context, OpenDALByteBufferResult result)
    {
        if (!AsyncStateRegistry.TryTake<ReadAsyncState>(context, out var state))
        {
            result.Error.Release();
            result.Buffer.Release();
            return;
        }

        try
        {
            state.CancellationRegistration.Dispose();

            if (result.Error.IsError)
            {
                state.Completion.TrySetException(new OpenDALException(result.Error));
                return;
            }

            if (result.Buffer.Data == IntPtr.Zero || result.Buffer.Len == 0)
            {
                state.Completion.TrySetResult(Array.Empty<byte>());
                return;
            }

            var size = checked((int)result.Buffer.Len);
            var managed = new byte[size];
            Marshal.Copy(result.Buffer.Data, managed, 0, size);
            state.Completion.TrySetResult(managed);
        }
        catch (Exception ex)
        {
            state.Completion.TrySetException(ex);
        }
        finally
        {
            result.Buffer.Release();
        }
    }

    /// <summary>
    /// Native callback invoked when an asynchronous stat operation finishes.
    /// </summary>
    /// <param name="context">Opaque async state context previously registered by <see cref="AsyncStateRegistry"/>.</param>
    /// <param name="result">Stat completion result returned by the native layer.</param>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnStatCompleted(IntPtr context, OpenDALMetadataResult result)
    {
        if (!AsyncStateRegistry.TryTake<StatAsyncState>(context, out var state))
        {
            result.Error.Release();
            if (result.Ptr != IntPtr.Zero)
            {
                NativeMethods.metadata_free(result.Ptr);
            }
            return;
        }

        try
        {
            state.CancellationRegistration.Dispose();

            if (result.Error.IsError)
            {
                state.Completion.TrySetException(new OpenDALException(result.Error));
                return;
            }

            state.Completion.TrySetResult(FromMetadataPointer(result.Ptr));
        }
        catch (Exception ex)
        {
            state.Completion.TrySetException(ex);
        }
    }

}