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

namespace OpenDAL.Logging;

/// <summary>
/// Receives log events from the native layer.
/// </summary>
/// <param name="evt">
/// The event. Only valid until this handler returns; copy out anything you keep.
/// </param>
/// <remarks>
/// Asynchronous operations report from native worker threads, so this runs
/// concurrently and must be thread-safe.
/// </remarks>
public delegate void OpenDALLogHandler(in OpenDALLogEvent evt);

/// <summary>
/// Process-wide sink and level control for <see cref="Layer.LoggingLayer"/>.
/// </summary>
/// <remarks>
/// Both the handler and the level are global, mirroring the Rust ecosystem's single
/// <c>log::max_level()</c>. Route per operator inside the handler using
/// <see cref="OpenDALLogEvent.Scheme"/> together with <see cref="OpenDALLogEvent.Name"/>
/// or <see cref="OpenDALLogEvent.Root"/>, since not every service reports a name.
/// </remarks>
public static class OpenDALLogging
{
    private static volatile OpenDALLogHandler? handler;

    // Nothing pushes this to the native side until the property is assigned, so it
    // has to match the native default. Change both or neither.
    private static OpenDALLogLevel minimumLevel = OpenDALLogLevel.Debug;

    /// <summary>
    /// Gets or sets the sink that receives every event passing <see cref="MinimumLevel"/>.
    /// </summary>
    /// <remarks>
    /// Events are dropped while this is <see langword="null"/>. Exceptions thrown
    /// by the handler are swallowed: letting one escape would cross the native
    /// boundary and abort the process.
    /// </remarks>
    public static OpenDALLogHandler? Handler
    {
        get => handler;
        set => handler = value;
    }

    /// <summary>
    /// Gets or sets the threshold above which events are discarded natively,
    /// before any marshalling happens.
    /// </summary>
    /// <remarks>
    /// A coarse gate, not an off switch: the native layer builds some context strings
    /// before consulting it, so not applying <see cref="Layer.LoggingLayer"/> is the only
    /// way to pay nothing. Assign it again from configuration-change callbacks to track
    /// the logging framework's own level.
    /// </remarks>
    public static OpenDALLogLevel MinimumLevel
    {
        get => minimumLevel;
        set
        {
            minimumLevel = value;
            NativeMethods.logging_set_min_level((byte)value);
        }
    }

    /// <summary>
    /// Gets the native callback pointer handed to the logging layer.
    /// </summary>
    internal static unsafe delegate* unmanaged[Cdecl]<OpenDALLogEventRef*, void> Callback =>
        &OnLog;

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void OnLog(OpenDALLogEventRef* native)
    {
        // An exception reaching native code here is undefined behavior and takes
        // the process down. The sink is user code running inline, so this guard
        // is a memory-safety requirement rather than politeness.
        try
        {
            var current = handler;
            if (current is null || native is null)
            {
                return;
            }

            var evt = new OpenDALLogEvent(in *native);
            current(in evt);
        }
        catch
        {
            // Intentionally swallowed.
        }
    }
}
