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

namespace DotOpenDAL;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

/// <summary>
/// Represents metadata of an OpenDAL operator.
/// </summary>
public sealed class OperatorInfo
{
    /// <summary>
    /// Gets the scheme of this operator.
    /// </summary>
    public string Scheme { get; }

    /// <summary>
    /// Gets the configured root of this operator.
    /// </summary>
    public string Root { get; }

    /// <summary>
    /// Gets the configured name of this operator.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the full capability of this operator.
    /// </summary>
    public Capability FullCapability { get; }

    /// <summary>
    /// Gets the native capability of this operator.
    /// </summary>
    public Capability NativeCapability { get; }

    internal OperatorInfo(string scheme, string root, string name, Capability fullCapability, Capability nativeCapability)
    {
        Scheme = scheme;
        Root = root;
        Name = name;
        FullCapability = fullCapability;
        NativeCapability = nativeCapability;
    }

    internal static unsafe OperatorInfo FromNativePointer(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
        {
            throw new InvalidOperationException("operator_info_get returned null pointer");
        }

        var payload = Unsafe.Read<OpenDALOperatorInfo>((void*)ptr);
        return new OperatorInfo(
            Utilities.ReadUtf8(payload.Scheme),
            Utilities.ReadUtf8(payload.Root),
            Utilities.ReadUtf8(payload.Name),
            payload.FullCapability,
            payload.NativeCapability);
    }
}

[StructLayout(LayoutKind.Sequential)]
/// <summary>
/// Native payload for operator metadata returned by <c>operator_info_get</c>.
/// </summary>
/// <remarks>
/// String fields are unmanaged UTF-8 pointers allocated by native code and are
/// released together via <c>operator_info_free</c>.
/// </remarks>
internal struct OpenDALOperatorInfo
{
    /// <summary>
    /// Backend scheme name pointer.
    /// </summary>
    public IntPtr Scheme;

    /// <summary>
    /// Backend root path pointer.
    /// </summary>
    public IntPtr Root;

    /// <summary>
    /// Backend display name pointer.
    /// </summary>
    public IntPtr Name;

    /// <summary>
    /// Full capability payload.
    /// </summary>
    public Capability FullCapability;

    /// <summary>
    /// Native capability payload.
    /// </summary>
    public Capability NativeCapability;
}