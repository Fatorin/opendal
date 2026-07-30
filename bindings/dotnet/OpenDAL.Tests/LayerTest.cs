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

using System.Collections.Concurrent;
using OpenDAL.Layer;
using OpenDAL.Logging;

namespace OpenDAL.Tests;

/// <summary>
/// Covers every layer, including the logging ones.
/// </summary>
/// <remarks>
/// The logging sink and level are process-wide, so every test that touches them
/// lives here. Keeping them in one class means xunit runs them sequentially
/// without needing a shared collection.
/// </remarks>
public class LayerTest : IDisposable
{
    private readonly ConcurrentQueue<LogRecord> logEvents = new();

    public void Dispose()
    {
        OpenDALLogging.Handler = null;
        OpenDALLogging.MinimumLevel = OpenDALLogLevel.Debug;
    }

    [Fact]
    public void WithConcurrentLimit_ReturnsNewOperator()
    {
        using var op = new Operator("memory");
        var before = op.Op;
        using var layered = op.WithLayer(new ConcurrentLimitLayer(4));

        Assert.NotEqual(IntPtr.Zero, layered.Op);
        Assert.NotSame(op, layered);
        Assert.Equal(before, op.Op);
        Assert.NotEqual(before, layered.Op);

        layered.Write("layer-concurrent", [1, 2, 3]);
        var value = layered.Read("layer-concurrent");
        Assert.Equal([1, 2, 3], value);
    }

    [Fact]
    public void WithConcurrentLimit_HttpPermits_ReturnsNewOperator()
    {
        using var op = new Operator("memory");
        var before = op.Op;
        using var layered = op.WithLayer(new ConcurrentLimitLayer(4, 2));

        Assert.NotEqual(IntPtr.Zero, layered.Op);
        Assert.NotSame(op, layered);
        Assert.Equal(before, op.Op);
        Assert.NotEqual(before, layered.Op);

        layered.Write("layer-concurrent-http", [1, 2, 3]);
        var value = layered.Read("layer-concurrent-http");
        Assert.Equal([1, 2, 3], value);
    }

    [Fact]
    public void WithConcurrentLimit_ZeroPermits_ThrowsArgumentOutOfRangeException()
    {
        using var op = new Operator("memory");

        Assert.Throws<ArgumentOutOfRangeException>(() => op.WithLayer(new ConcurrentLimitLayer(0)));
    }

    [Fact]
    public void WithConcurrentLimit_OmittedHttpPermits_LeavesHttpLimitUnset()
    {
        Assert.Null(new ConcurrentLimitLayer(4).HttpPermits);
        Assert.Equal((nuint)2, new ConcurrentLimitLayer(4, 2).HttpPermits);
    }

    [Fact]
    public void WithConcurrentLimit_ZeroHttpPermits_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ConcurrentLimitLayer(4, 0));
    }

    [Fact]
    public void WithRetry_ReturnsNewOperator()
    {
        using var op = new Operator("memory");
        var before = op.Op;
        using var layered = op.WithLayer(new RetryLayer
        {
            Jitter = false,
            Factor = 2,
            MinDelay = TimeSpan.FromMilliseconds(1),
            MaxDelay = TimeSpan.FromMilliseconds(10),
            MaxTimes = 2,
        });

        Assert.NotEqual(IntPtr.Zero, layered.Op);
        Assert.NotSame(op, layered);
        Assert.Equal(before, op.Op);
        Assert.NotEqual(before, layered.Op);

        layered.Write("layer-retry", [4, 5, 6]);
        var value = layered.Read("layer-retry");
        Assert.Equal([4, 5, 6], value);
    }

    [Fact]
    public void WithRetry_InvalidFactor_ThrowsArgumentOutOfRangeException()
    {
        using var op = new Operator("memory");

        Assert.Throws<ArgumentOutOfRangeException>(() => op.WithLayer(new RetryLayer
        {
            Factor = 0,
        }));
    }

    [Fact]
    public void WithTimeout_ReturnsNewOperator()
    {
        using var op = new Operator("memory");
        var before = op.Op;
        using var layered = op.WithLayer(new TimeoutLayer
        {
            Timeout = TimeSpan.FromSeconds(5),
            IoTimeout = TimeSpan.FromSeconds(2),
        });

        Assert.NotEqual(IntPtr.Zero, layered.Op);
        Assert.NotSame(op, layered);
        Assert.Equal(before, op.Op);
        Assert.NotEqual(before, layered.Op);

        layered.Write("layer-timeout", [7, 8, 9]);
        var value = layered.Read("layer-timeout");
        Assert.Equal([7, 8, 9], value);
    }

    [Fact]
    public void WithTimeout_ZeroTimeout_ThrowsArgumentOutOfRangeException()
    {
        using var op = new Operator("memory");

        Assert.Throws<ArgumentOutOfRangeException>(() => op.WithLayer(new TimeoutLayer
        {
            Timeout = TimeSpan.Zero,
        }));
    }

    [Fact]
    public void WithThrottle_ReturnsNewOperator()
    {
        using var op = new Operator("memory");
        var before = op.Op;
        using var layered = op.WithLayer(new ThrottleLayer(10 * 1024, 10 * 1024 * 1024));

        Assert.NotEqual(IntPtr.Zero, layered.Op);
        Assert.NotSame(op, layered);
        Assert.Equal(before, op.Op);
        Assert.NotEqual(before, layered.Op);

        layered.Write("layer-throttle", [1, 2, 3]);
        var value = layered.Read("layer-throttle");
        Assert.Equal([1, 2, 3], value);
    }

    [Theory]
    [InlineData(0u, 1024u)]
    [InlineData(1024u, 0u)]
    public void WithThrottle_ZeroArgument_ThrowsArgumentOutOfRangeException(uint bandwidth, uint burst)
    {
        using var op = new Operator("memory");

        Assert.Throws<ArgumentOutOfRangeException>(() => op.WithLayer(new ThrottleLayer(bandwidth, burst)));
    }

    [Fact]
    public void WithMimeGuess_FillsContentTypeFromExtension()
    {
        using var op = new Operator("memory");
        var before = op.Op;
        using var layered = op.WithLayer(new MimeGuessLayer());

        Assert.NotEqual(IntPtr.Zero, layered.Op);
        Assert.NotSame(op, layered);
        Assert.Equal(before, op.Op);
        Assert.NotEqual(before, layered.Op);

        layered.Write("layer-mime-guess.json", [1, 2, 3]);
        Assert.Equal("application/json", layered.Stat("layer-mime-guess.json").ContentType);
    }

    [Fact]
    public void WithMimeGuess_DoesNotOverrideExplicitContentType()
    {
        using var op = new Operator("memory");
        using var layered = op.WithLayer(new MimeGuessLayer());

        layered.Write(
            "layer-mime-guess-explicit.json",
            [1, 2, 3],
            new OpenDAL.Options.WriteOptions { ContentType = "text/plain" });

        Assert.Equal("text/plain", layered.Stat("layer-mime-guess-explicit.json").ContentType);
    }

    [Fact]
    public void WithMimeGuess_UnknownExtension_LeavesContentTypeUnset()
    {
        using var op = new Operator("memory");
        using var layered = op.WithLayer(new MimeGuessLayer());

        layered.Write("layer-mime-guess.no-such-ext", [1, 2, 3]);

        Assert.Null(layered.Stat("layer-mime-guess.no-such-ext").ContentType);
    }

    [Fact]
    public void WithLayer_OperatorsCanBeDisposedIndependently()
    {
        var op = new Operator("memory");
        var layered = op.WithLayer(new ConcurrentLimitLayer(2));

        layered.Dispose();

        op.Write("layer-dispose-origin", [1, 1, 1]);
        var originalValue = op.Read("layer-dispose-origin");
        Assert.Equal([1, 1, 1], originalValue);

        op.Dispose();

        var op2 = new Operator("memory");
        var layered2 = op2.WithLayer(new ConcurrentLimitLayer(2));

        op2.Dispose();

        layered2.Write("layer-dispose-layered", [2, 2, 2]);
        var layeredValue = layered2.Read("layer-dispose-layered");
        Assert.Equal([2, 2, 2], layeredValue);

        layered2.Dispose();
    }

    #region Logging

    private sealed record LogRecord(
        OpenDALLogLevel Level,
        OpenDALOperation Operation,
        string Scheme,
        string Root,
        string Message,
        IReadOnlyDictionary<string, string> Context,
        ErrorCode? ErrorCode,
        string? ErrorMessage);

    private void CaptureLog(in OpenDALLogEvent evt)
    {
        var context = new Dictionary<string, string>();
        for (var i = 0; i < evt.ContextCount; i++)
        {
            context[System.Text.Encoding.UTF8.GetString(evt.GetContextKey(i))] =
                System.Text.Encoding.UTF8.GetString(evt.GetContextValue(i));
        }

        ErrorCode? code = null;
        string? errorMessage = null;
        if (evt.TryGetError(out var errorCode, out var message))
        {
            code = errorCode;
            errorMessage = System.Text.Encoding.UTF8.GetString(message);
        }

        logEvents.Enqueue(new LogRecord(
            evt.Level,
            evt.Operation,
            System.Text.Encoding.UTF8.GetString(evt.Scheme),
            System.Text.Encoding.UTF8.GetString(evt.Root),
            System.Text.Encoding.UTF8.GetString(evt.Message),
            context,
            code,
            errorMessage));
    }

    [Fact]
    public void WithLogging_ReturnsNewOperator()
    {
        OpenDALLogging.Handler = CaptureLog;

        using var op = new Operator("memory");
        var before = op.Op;
        using var layered = op.WithLayer(new LoggingLayer());

        Assert.NotEqual(IntPtr.Zero, layered.Op);
        Assert.NotSame(op, layered);
        Assert.Equal(before, op.Op);
        Assert.NotEqual(before, layered.Op);
    }

    [Fact]
    public void WithLogging_EmitsEventsCarryingSchemeAndContext()
    {
        OpenDALLogging.Handler = CaptureLog;

        using var op = new Operator("memory");
        using var layered = op.WithLayer(new LoggingLayer());

        layered.Write("layer-logging-basic", [1, 2, 3]);

        // One write reports several stages: started, created writer, close succeeded.
        var writes = logEvents
            .Where(e => e.Operation == OpenDALOperation.Write && e.Context.ContainsKey("path"))
            .ToArray();

        Assert.NotEmpty(writes);
        Assert.All(writes, e => Assert.Equal("memory", e.Scheme));

        // Root comes after Name in the native struct, so reading it back also
        // confirms the two sides still agree on the layout.
        Assert.All(writes, e => Assert.Equal("/", e.Root));
        Assert.All(writes, e => Assert.Equal("layer-logging-basic", e.Context["path"]));
        Assert.All(writes, e => Assert.Equal(OpenDALLogLevel.Debug, e.Level));
        Assert.Contains(writes, e => e.Message == "started");
    }

    [Fact]
    public void WithLogging_WarningThreshold_KeepsFailuresAndDropsTheRest()
    {
        OpenDALLogging.Handler = CaptureLog;

        using var op = new Operator("memory");
        using var layered = op.WithLayer(new LoggingLayer());

        // Levels follow log::LevelFilter, so a larger value is more verbose and an
        // event passes when it is at or below the threshold. Asserting both sides
        // pins that direction down.
        OpenDALLogging.MinimumLevel = OpenDALLogLevel.Warning;

        layered.Write("layer-logging-threshold", [1, 2, 3]);
        Assert.Empty(logEvents);

        Assert.Throws<OpenDALException>(() => layered.Read("layer-logging-absent"));
        Assert.NotEmpty(logEvents);
        Assert.All(logEvents, e => Assert.Equal(OpenDALLogLevel.Warning, e.Level));
    }

    [Fact]
    public void WithLogging_LevelIsProcessWide_NotPerOperator()
    {
        OpenDALLogging.Handler = CaptureLog;

        using var first = new Operator("memory");
        using var firstLayered = first.WithLayer(new LoggingLayer());
        using var second = new Operator("memory");
        using var secondLayered = second.WithLayer(new LoggingLayer());

        // One level for the whole process, so silencing it through either operator
        // silences both. Changing it after the layer is applied also has to work,
        // since the gate is read per event rather than captured at apply time.
        OpenDALLogging.MinimumLevel = OpenDALLogLevel.Off;
        firstLayered.Write("layer-logging-global-a", [1, 2, 3]);
        secondLayered.Write("layer-logging-global-b", [1, 2, 3]);
        Assert.Empty(logEvents);

        OpenDALLogging.MinimumLevel = OpenDALLogLevel.Debug;
        firstLayered.Write("layer-logging-global-a", [4, 5, 6]);
        secondLayered.Write("layer-logging-global-b", [4, 5, 6]);

        var paths = logEvents
            .Where(e => e.Context.TryGetValue("path", out _))
            .Select(e => e.Context["path"])
            .Distinct()
            .ToArray();

        Assert.Contains("layer-logging-global-a", paths);
        Assert.Contains("layer-logging-global-b", paths);
    }

    [Fact]
    public void WithLogging_FailedOperation_CarriesErrorAndRebuildsIntoException()
    {
        OpenDALException? rebuilt = null;
        OpenDALLogging.Handler = (in OpenDALLogEvent evt) =>
        {
            CaptureLog(in evt);

            // Mirrors the ILogger bridge documented in the .NET production guide,
            // which is the only consumer of the (ErrorCode, string) constructor.
            if (evt.TryGetError(out var code, out var errorMessage))
            {
                rebuilt = new OpenDALException(
                    code,
                    System.Text.Encoding.UTF8.GetString(errorMessage));
            }
        };

        using var op = new Operator("memory");
        using var layered = op.WithLayer(new LoggingLayer());

        var thrown = Assert.Throws<OpenDALException>(
            () => layered.Read("layer-logging-missing-key"));

        var failed = logEvents.Where(e => e.ErrorCode is not null).ToArray();
        Assert.NotEmpty(failed);
        Assert.All(failed, e => Assert.False(string.IsNullOrEmpty(e.ErrorMessage)));

        // NotFound is expected rather than unexpected, so it is reported below Error.
        Assert.All(failed, e => Assert.Equal(OpenDALLogLevel.Warning, e.Level));

        // A code seen in a log event and one caught from the call must agree,
        // otherwise log filters and catch blocks would need separate rules.
        Assert.NotNull(rebuilt);
        Assert.Equal(OpenDAL.ErrorCode.NotFound, rebuilt.Code);
        Assert.Equal(thrown.Code, rebuilt.Code);
    }

    [Fact]
    public void WithLogging_ThrowingHandler_DoesNotCrashTheProcess()
    {
        OpenDALLogging.Handler = static (in OpenDALLogEvent evt) =>
            throw new InvalidOperationException("sink failure");

        using var op = new Operator("memory");
        using var layered = op.WithLayer(new LoggingLayer());

        layered.Write("layer-logging-throwing-sink", [4, 5, 6]);
        var value = layered.Read("layer-logging-throwing-sink");

        Assert.Equal([4, 5, 6], value);
    }

    #endregion
}
