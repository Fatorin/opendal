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
using DotOpenDAL.Interop.Result.Abstractions;

namespace DotOpenDAL.Interop.Result;

[StructLayout(LayoutKind.Sequential)]
/// <summary>
/// Result wrapper for operations that return an operator pointer payload.
/// </summary>
internal struct OpenDALOperatorResult
    : INativeValueResult<IntPtr>
{
    /// <summary>
    /// Native pointer payload on success.
    /// </summary>
    public IntPtr Ptr;

    /// <summary>
    /// Error details for the operation.
    /// </summary>
    public OpenDALError Error;

    /// <summary>
    /// Releases native resources referenced by <see cref="Error"/>.
    /// </summary>
    public readonly void Release()
    {
        NativeMethods.opendal_error_release(Error);
    }

    /// <summary>
    /// Gets operation error details returned by native code.
    /// </summary>
    /// <returns>The native error payload.</returns>
    public readonly OpenDALError GetError()
    {
        return Error;
    }

    /// <summary>
    /// Returns the native operator pointer payload.
    /// </summary>
    /// <returns>Native operator pointer.</returns>
    public readonly IntPtr ToValue()
    {
        return Ptr;
    }
}
