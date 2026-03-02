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
using DotOpenDAL.Layer.Abstractions;
using DotOpenDAL.Options;
using DotOpenDAL.Options.Abstractions;
using DotOpenDAL.ServiceConfig.Abstractions;

namespace DotOpenDAL;

/// <summary>
/// Managed wrapper over an OpenDAL native operator handle.
/// </summary>
public partial class Operator : SafeHandle
{
    private static readonly IReadOnlyDictionary<string, string> EmptyNativeOptions = new Dictionary<string, string>();
    private Lazy<OperatorInfo> info;

    private Operator() : base(IntPtr.Zero, true)
    {
        info = CreateInfoLazy();
    }

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
            return info.Value;
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
        info = CreateInfoLazy();

        using var nativeOptionsHandle = CreateNativeOptionsHandle(options ?? EmptyNativeOptions);
        var result = NativeMethods.operator_construct(scheme, nativeOptionsHandle.Ptr);

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
    /// Applies the specified layer and returns this operator instance.
    /// </summary>
    /// <param name="layer">Layer to apply.</param>
    /// <returns>This operator with the layer applied.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="layer"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The operator has been disposed.</exception>
    /// <exception cref="OpenDALException">Native layer application fails.</exception>
    public Operator WithLayer(ILayer layer)
    {
        ArgumentNullException.ThrowIfNull(layer);
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        return layer.Apply(this);
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
        Write(path, content, options: null, executor: null);
    }

    /// <summary>
    /// Writes the specified content to a path with write options.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <param name="options">Additional write options.</param>
    public void Write(string path, byte[] content, WriteOptions options)
    {
        Write(path, content, (WriteOptions?)options, executor: null);
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
        Write(path, content, options: null, executor);
    }

    /// <summary>
    /// Writes the specified content to a path with write options using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <param name="options">Additional write options.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    public void Write(string path, byte[] content, WriteOptions? options, Executor? executor)
    {
        ArgumentNullException.ThrowIfNull(content);
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);
        var nativeOptions = ToNativeOptions(options);

        OpenDALResult result;
        unsafe
        {
            using var nativeOptionsHandle = CreateNativeOptionsHandle(nativeOptions);
            fixed (byte* contentPtr = content)
            {
                result = NativeMethods.operator_write_with_options(
                    this,
                    executorHandle,
                    path,
                    contentPtr,
                    (nuint)content.Length,
                    nativeOptionsHandle.Ptr);
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
    public Task WriteAsync(string path, byte[] content, CancellationToken cancellationToken = default)
    {
        return WriteAsync(path, content, options: null, executor: null, cancellationToken);
    }

    /// <summary>
    /// Writes the specified content to a path asynchronously with write options.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <param name="options">Additional write options.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    public Task WriteAsync(
        string path,
        byte[] content,
        WriteOptions options,
        CancellationToken cancellationToken = default)
    {
        return WriteAsync(path, content, (WriteOptions?)options, executor: null, cancellationToken);
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
    public Task WriteAsync(
        string path,
        byte[] content,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        return WriteAsync(path, content, options: null, executor, cancellationToken);
    }

    /// <summary>
    /// Writes the specified content to a path asynchronously with optional write options and executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <param name="options">Additional write options, or <see langword="null"/> for default behavior.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    public unsafe Task WriteAsync(
        string path,
        byte[] content,
        WriteOptions? options,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        cancellationToken.ThrowIfCancellationRequested();
        var executorHandle = GetExecutorHandle(executor);
        var nativeOptions = ToNativeOptions(options);

        var context = AsyncStateRegistry.Register<object?>(out var asyncState);

        try
        {
            OpenDALResult submitResult;
            unsafe
            {
                using var nativeOptionsHandle = CreateNativeOptionsHandle(nativeOptions);
                fixed (byte* contentPtr = content)
                {
                    submitResult = NativeMethods.operator_write_with_options_async(
                        this,
                        executorHandle,
                        path,
                        contentPtr,
                        (nuint)content.Length,
                        nativeOptionsHandle.Ptr,
                        &OnWriteCompleted,
                        new IntPtr(context));
                }
            }

            if (submitResult.Error.IsError)
            {
                AsyncStateRegistry.Unregister(context);
                throw new OpenDALException(submitResult.Error);
            }

            asyncState.BindCancellation(cancellationToken);
            return asyncState.Completion.Task;
        }
        catch
        {
            AsyncStateRegistry.Unregister(context);
            throw;
        }
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
        return Read(path, options: null, executor: null);
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
        return Read(path, options: null, executor);
    }

    /// <summary>
    /// Reads bytes from a path with read options using the provided executor.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <param name="options">Additional read options.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <returns>The content bytes.</returns>
    public byte[] Read(string path, ReadOptions? options, Executor? executor)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);
        var nativeOptions = ToNativeOptions(options);

        OpenDALByteBufferResult result;
        unsafe
        {
            using var nativeOptionsHandle = CreateNativeOptionsHandle(nativeOptions);
            result = NativeMethods.operator_read_with_options(this, executorHandle, path, nativeOptionsHandle.Ptr);
        }

        if (result.Error.IsError)
        {
            throw new OpenDALException(result.Error);
        }

        try
        {
            return result.Buffer.ToManagedBytes();
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
    public Task<byte[]> ReadAsync(string path, CancellationToken cancellationToken = default)
    {
        return ReadAsync(path, options: null, executor: null, cancellationToken);
    }

    /// <summary>
    /// Reads bytes from a path asynchronously with read options.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <param name="options">Additional read options.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that resolves with the read content.</returns>
    public Task<byte[]> ReadAsync(string path, ReadOptions options, CancellationToken cancellationToken = default)
    {
        return ReadAsync(path, (ReadOptions?)options, executor: null, cancellationToken);
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
    public Task<byte[]> ReadAsync(
        string path,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        return ReadAsync(path, options: null, executor, cancellationToken);
    }

    /// <summary>
    /// Reads bytes from a path asynchronously with optional read options and executor.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <param name="options">Additional read options, or <see langword="null"/> for default behavior.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that resolves with the read content.</returns>
    public unsafe Task<byte[]> ReadAsync(
        string path,
        ReadOptions? options,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        cancellationToken.ThrowIfCancellationRequested();
        var executorHandle = GetExecutorHandle(executor);
        var nativeOptions = ToNativeOptions(options);

        var context = AsyncStateRegistry.Register<byte[]>(out var asyncState);

        try
        {
            OpenDALResult submitResult;
            unsafe
            {
                using var nativeOptionsHandle = CreateNativeOptionsHandle(nativeOptions);
                submitResult = NativeMethods.operator_read_with_options_async(
                    this,
                    executorHandle,
                    path,
                    nativeOptionsHandle.Ptr,
                    &OnReadCompleted,
                    new IntPtr(context));
            }

            if (submitResult.Error.IsError)
            {
                AsyncStateRegistry.Unregister(context);
                throw new OpenDALException(submitResult.Error);
            }

            asyncState.BindCancellation(cancellationToken);
            return asyncState.Completion.Task;
        }
        catch
        {
            AsyncStateRegistry.Unregister(context);
            throw;
        }
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
        var nativeOptions = ToNativeOptions(options);

        OpenDALPointerResult result;
        unsafe
        {
            using var nativeOptionsHandle = CreateNativeOptionsHandle(nativeOptions);
            result = NativeMethods.operator_stat_with_options(this, executorHandle, path, nativeOptionsHandle.Ptr);
        }
        if (result.Error.IsError)
        {
            throw new OpenDALException(result.Error);
        }

        return Metadata.FromNativePointer(result.Ptr);
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
        var nativeOptions = ToNativeOptions(options);

        var context = AsyncStateRegistry.Register<Metadata>(out var asyncState);

        try
        {
            OpenDALResult submitResult;
            unsafe
            {
                using var nativeOptionsHandle = CreateNativeOptionsHandle(nativeOptions);
                submitResult = NativeMethods.operator_stat_with_options_async(
                    this,
                    executorHandle,
                    path,
                    nativeOptionsHandle.Ptr,
                    &OnStatCompleted,
                    new IntPtr(context));
            }

            if (submitResult.Error.IsError)
            {
                AsyncStateRegistry.Unregister(context);
                throw new OpenDALException(submitResult.Error);
            }

            asyncState.BindCancellation(cancellationToken);
            return asyncState.Completion.Task;
        }
        catch
        {
            AsyncStateRegistry.Unregister(context);
            throw;
        }
    }

    /// <summary>
    /// Lists entries under the specified path.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="options">Additional list options.</param>
    /// <returns>Listed entries.</returns>
    public IReadOnlyList<Entry> List(string path, ListOptions? options = null)
    {
        return List(path, options, executor: null);
    }

    /// <summary>
    /// Lists entries under the specified path using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="options">Additional list options.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <returns>Listed entries.</returns>
    public IReadOnlyList<Entry> List(string path, ListOptions? options, Executor? executor)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);
        var nativeOptions = ToNativeOptions(options);

        OpenDALPointerResult result;
        unsafe
        {
            using var nativeOptionsHandle = CreateNativeOptionsHandle(nativeOptions);
            result = NativeMethods.operator_list_with_options(this, executorHandle, path, nativeOptionsHandle.Ptr);
        }

        if (result.Error.IsError)
        {
            throw new OpenDALException(result.Error);
        }

        return Entry.FromNativePointer(result.Ptr);
    }

    /// <summary>
    /// Lists entries under the specified path asynchronously.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="options">Additional list options.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that resolves with listed entries.</returns>
    public Task<IReadOnlyList<Entry>> ListAsync(
        string path,
        ListOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        return ListAsync(path, options, executor: null, cancellationToken);
    }

    /// <summary>
    /// Lists entries under the specified path asynchronously using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="options">Additional list options.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that resolves with listed entries.</returns>
    public Task<IReadOnlyList<Entry>> ListAsync(
        string path,
        ListOptions? options,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        cancellationToken.ThrowIfCancellationRequested();
        var executorHandle = GetExecutorHandle(executor);

        var context = AsyncStateRegistry.Register<IReadOnlyList<Entry>>(out var asyncState);

        try
        {
            using var nativeOptions = CreateNativeOptionsHandle(ToNativeOptions(options));
            unsafe
            {
                OpenDALResult submitResult = NativeMethods.operator_list_with_options_async(
                    this,
                    executorHandle,
                    path,
                    nativeOptions.Ptr,
                    &OnListCompleted,
                    new IntPtr(context));

                if (submitResult.Error.IsError)
                {
                    AsyncStateRegistry.Unregister(context);
                    throw new OpenDALException(submitResult.Error);
                }
            }

            asyncState.BindCancellation(cancellationToken);
            return asyncState.Completion.Task;
        }
        catch
        {
            AsyncStateRegistry.Unregister(context);
            throw;
        }
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

    internal Operator ApplyLayerResult(OpenDALPointerResult result)
    {
        if (result.Error.IsError)
        {
            throw new OpenDALException(result.Error);
        }

        if (result.Ptr == IntPtr.Zero)
        {
            throw new InvalidOperationException("Layer application returned null operator pointer");
        }

        var oldHandle = handle;
        SetHandle(result.Ptr);
        info = CreateInfoLazy();

        if (oldHandle != IntPtr.Zero)
        {
            NativeMethods.operator_free(oldHandle);
        }

        return this;
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

    private Lazy<OperatorInfo> CreateInfoLazy()
    {
        return new Lazy<OperatorInfo>(
            () => OperatorInfo.FromNativePointerResult(NativeMethods.operator_info_get(this)),
            LazyThreadSafetyMode.ExecutionAndPublication);
    }

    private static IReadOnlyDictionary<string, string> ToNativeOptions(IOptions? options)
    {
        return options is null ? EmptyNativeOptions : options.ToNativeOptions();
    }

    private static NativeOptionsHandle CreateNativeOptionsHandle(IReadOnlyDictionary<string, string> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var handle = NativeMethods.operator_options_new();
        if (handle == IntPtr.Zero)
        {
            throw new InvalidOperationException("Failed to allocate native options handle");
        }

        try
        {
            foreach (var option in options)
            {
                var setResult = NativeMethods.operator_options_set(handle, option.Key, option.Value);
                if (setResult.Error.IsError)
                {
                    throw new OpenDALException(setResult.Error);
                }
            }

            return new NativeOptionsHandle(handle);
        }
        catch
        {
            NativeMethods.operator_options_free(handle);
            throw;
        }
    }

    private sealed class NativeOptionsHandle : IDisposable
    {
        private IntPtr ptr;

        public NativeOptionsHandle(IntPtr ptr)
        {
            this.ptr = ptr;
        }

        public IntPtr Ptr => ptr;

        public void Dispose()
        {
            if (ptr == IntPtr.Zero)
            {
                return;
            }

            NativeMethods.operator_options_free(ptr);
            ptr = IntPtr.Zero;
        }
    }

    #region Async Helpers

    private static bool TryTakeAsyncState<T>(IntPtr context, OpenDALError error, out AsyncState<T>? state)
    {
        if (AsyncStateRegistry.TryTake<AsyncState<T>>(context, out var current))
        {
            state = current;
            return true;
        }

        error.Release();
        state = null;
        return false;
    }

    private static bool TryTakeAsyncState<T, TState>(
        IntPtr context,
        OpenDALError error,
        TState onMissingState,
        Action<TState> onMissing,
        out AsyncState<T>? state)
    {
        if (TryTakeAsyncState(context, error, out state))
        {
            return true;
        }

        onMissing(onMissingState);
        return false;
    }

    private static void CompleteAsyncCallback<TOutput>(
        AsyncState<TOutput> state,
        OpenDALError error,
        TOutput result)
    {
        CompleteAsyncCallback(state, error, result, static value => value);
    }

    private static void CompleteAsyncCallback<TInput, TOutput>(
        AsyncState<TOutput> state,
        OpenDALError error,
        TInput input,
        Func<TInput, TOutput> resultFactory)
    {
        try
        {
            state.CancellationRegistration.Dispose();

            if (error.IsError)
            {
                state.Completion.TrySetException(new OpenDALException(error));
                return;
            }

            state.Completion.TrySetResult(resultFactory(input));
        }
        catch (Exception ex)
        {
            state.Completion.TrySetException(ex);
        }
    }

    #endregion

    #region Async Callbacks

    /// <summary>
    /// Native callback invoked when an asynchronous write operation finishes.
    /// </summary>
    /// <param name="context">Opaque async state context previously registered by <see cref="AsyncStateRegistry"/>.</param>
    /// <param name="result">Write completion result returned by the native layer.</param>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnWriteCompleted(IntPtr context, OpenDALResult result)
    {
        if (!TryTakeAsyncState(context, result.Error, out AsyncState<object?>? state))
        {
            return;
        }

        CompleteAsyncCallback(state!, result.Error, null);
    }

    /// <summary>
    /// Native callback invoked when an asynchronous read operation finishes.
    /// </summary>
    /// <param name="context">Opaque async state context previously registered by <see cref="AsyncStateRegistry"/>.</param>
    /// <param name="result">Read completion result returned by the native layer, including byte buffer payload.</param>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnReadCompleted(IntPtr context, OpenDALByteBufferResult result)
    {
        if (!TryTakeAsyncState(
                context,
                result.Error,
            result.Buffer,
            static buffer => buffer.Release(),
                out AsyncState<byte[]>? state))
        {
            return;
        }

        try
        {
            CompleteAsyncCallback(state!, result.Error, result.Buffer, static buffer => buffer.ToManagedBytes());
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
    private static void OnStatCompleted(IntPtr context, OpenDALPointerResult result)
    {
        if (!TryTakeAsyncState(
            context,
            result.Error,
            result.Ptr,
            static ptr => NativeMethods.metadata_free(ptr),
            out AsyncState<Metadata>? state))
        {
            return;
        }

        CompleteAsyncCallback(state!, result.Error, result.Ptr, Metadata.FromNativePointer);
    }

    /// <summary>
    /// Native callback invoked when an asynchronous list operation finishes.
    /// </summary>
    /// <param name="context">Opaque async state context previously registered by <see cref="AsyncStateRegistry"/>.</param>
    /// <param name="result">List completion result returned by the native layer.</param>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnListCompleted(IntPtr context, OpenDALPointerResult result)
    {
        if (!TryTakeAsyncState(context, result.Error, result.Ptr, static ptr => NativeMethods.entry_list_free(ptr), out AsyncState<IReadOnlyList<Entry>>? state))
        {
            return;
        }

        CompleteAsyncCallback(state!, result.Error, result.Ptr, Entry.FromNativePointer);
    }

    #endregion

}