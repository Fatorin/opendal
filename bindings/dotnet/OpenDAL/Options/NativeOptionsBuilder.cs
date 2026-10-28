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


using System.Buffers;
using System.Text.Json;
using OpenDAL.Options.Abstractions;

namespace OpenDAL.Options;

/// <summary>
/// Builds options as the JSON object that native entry points decode.
/// </summary>
/// <remarks>
/// Native code rejects unknown members, so every key added here must match
/// the native mirror generated from the core options struct.
/// </remarks>
internal readonly struct NativeOptionsBuilder
{
    private readonly Utf8JsonWriter writer;

    private NativeOptionsBuilder(Utf8JsonWriter writer)
    {
        this.writer = writer;
    }

    /// <summary>
    /// Adds the option as <see langword="true"/> when <paramref name="value"/> is true.
    /// </summary>
    public NativeOptionsBuilder AddBoolTrue(string key, bool value)
    {
        if (value)
        {
            writer.WriteBoolean(key, true);
        }

        return this;
    }

    /// <summary>
    /// Adds the option when the string value is not null or empty.
    /// </summary>
    public NativeOptionsBuilder AddString(string key, string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            writer.WriteString(key, value);
        }

        return this;
    }

    /// <summary>
    /// Adds the option when the int value differs from the specified default.
    /// </summary>
    public NativeOptionsBuilder AddInt32IfNotDefault(string key, int value, int defaultValue)
    {
        if (value != defaultValue)
        {
            writer.WriteNumber(key, value);
        }

        return this;
    }

    /// <summary>
    /// Adds the option when the long value differs from the specified default.
    /// </summary>
    public NativeOptionsBuilder AddInt64IfNotDefault(string key, long value, long defaultValue)
    {
        if (value != defaultValue)
        {
            writer.WriteNumber(key, value);
        }

        return this;
    }

    /// <summary>
    /// Adds the option when the nullable long value is provided.
    /// </summary>
    public NativeOptionsBuilder AddNullableInt64(string key, long? value)
    {
        if (value is not null)
        {
            writer.WriteNumber(key, value.Value);
        }

        return this;
    }

    /// <summary>
    /// Adds the option as Unix time milliseconds when the timestamp is provided.
    /// </summary>
    public NativeOptionsBuilder AddUnixTimeMilliseconds(string key, DateTimeOffset? value)
    {
        if (value is not null)
        {
            writer.WriteNumber(key, value.Value.ToUnixTimeMilliseconds());
        }

        return this;
    }

    /// <summary>
    /// Adds the entries as a nested object when the map is provided.
    /// </summary>
    public NativeOptionsBuilder AddStringMap(string key, IReadOnlyDictionary<string, string>? values)
    {
        if (values is null)
        {
            return this;
        }

        writer.WriteStartObject(key);
        foreach (var entry in values)
        {
            writer.WriteString(entry.Key, entry.Value);
        }

        writer.WriteEndObject();
        return this;
    }

    /// <summary>
    /// Adds the metadata as a nested object when it is provided.
    /// </summary>
    /// <remarks>
    /// Every member of the metadata is written, so native code rebuilds the
    /// same metadata core compares against without the binding tracking which
    /// members a condition such as <c>if_not_changed</c> reads.
    /// </remarks>
    public NativeOptionsBuilder AddMetadata(string key, Metadata? value)
    {
        if (value is null)
        {
            return this;
        }

        writer.WritePropertyName(key);
        value.WriteJson(writer);
        return this;
    }

    /// <summary>
    /// Serializes options as a null-terminated UTF-8 JSON object, or returns
    /// <see langword="null"/> for defaults.
    /// </summary>
    public static byte[]? Serialize(IOptions? options)
    {
        if (options is null)
        {
            return null;
        }

        return WriteNative(options, static (writer, options) => WriteObject(writer, options));
    }

    /// <summary>
    /// Serializes string options as a null-terminated UTF-8 JSON object, or
    /// returns <see langword="null"/> when there are none.
    /// </summary>
    /// <remarks>
    /// A null value is written as JSON <c>null</c>, which native code rejects
    /// with <see cref="ErrorCode.ConfigInvalid"/> naming the key.
    /// </remarks>
    public static byte[]? Serialize(IReadOnlyDictionary<string, string>? options)
    {
        if (options is null || options.Count == 0)
        {
            return null;
        }

        return WriteNative(options, static (writer, options) =>
        {
            writer.WriteStartObject();
            foreach (var entry in options)
            {
                writer.WriteString(entry.Key, entry.Value);
            }

            writer.WriteEndObject();
        });
    }

    private static void WriteObject(Utf8JsonWriter writer, IOptions options)
    {
        writer.WriteStartObject();
        options.Build(new NativeOptionsBuilder(writer));
        writer.WriteEndObject();
    }

    // Each thread reuses one writer and buffer, so serializing allocates
    // only the returned array. A buffer grown past this size is dropped
    // instead of being kept alive for the thread's lifetime.
    private const int RetainedBufferCapacity = 16 * 1024;

    [ThreadStatic]
    private static ArrayBufferWriter<byte>? threadBuffer;

    [ThreadStatic]
    private static Utf8JsonWriter? threadWriter;

    [ThreadStatic]
    private static bool threadWriterInUse;

    /// <summary>
    /// Runs <paramref name="write"/> and returns its JSON as a null-terminated
    /// UTF-8 array that native code reads as a C string.
    /// </summary>
    /// <remarks>
    /// A call that re-enters while the thread's writer is busy, for example
    /// from a caller's dictionary that calls back into OpenDAL, gets a writer
    /// of its own instead of resetting the busy one.
    /// </remarks>
    private static byte[] WriteNative<TState>(TState state, Action<Utf8JsonWriter, TState> write)
    {
        if (threadWriterInUse)
        {
            var ownBuffer = new ArrayBufferWriter<byte>();
            using (var ownWriter = new Utf8JsonWriter(ownBuffer))
            {
                write(ownWriter, state);
            }

            return ToNativeString(ownBuffer);
        }

        var buffer = threadBuffer ??= new ArrayBufferWriter<byte>(256);
        buffer.ResetWrittenCount();
        var writer = threadWriter;
        if (writer is null)
        {
            writer = threadWriter = new Utf8JsonWriter(buffer);
        }
        else
        {
            writer.Reset(buffer);
        }

        threadWriterInUse = true;
        try
        {
            write(writer, state);
            writer.Flush();
            return ToNativeString(buffer);
        }
        finally
        {
            threadWriterInUse = false;
            if (buffer.Capacity > RetainedBufferCapacity)
            {
                threadBuffer = null;
                threadWriter = null;
            }
        }
    }

    private static byte[] ToNativeString(ArrayBufferWriter<byte> buffer)
    {
        var bytes = new byte[buffer.WrittenCount + 1];
        buffer.WrittenSpan.CopyTo(bytes);
        return bytes;
    }
}
