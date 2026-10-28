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

using OpenDAL.Options;

namespace OpenDAL;

/// <summary>
/// One complete source object of a compose operation.
/// </summary>
/// <remarks>
/// A plain path converts implicitly, so <c>op.Compose(["a", "b"], "target")</c>
/// composes the current versions of <c>a</c> and <c>b</c>.
/// </remarks>
public sealed class ComposeSource
{
    /// <summary>
    /// Initializes a compose source.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <param name="options">Options that select and guard this source.</param>
    public ComposeSource(string path, ComposeSourceOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(path);
        Path = path;
        Options = options;
    }

    /// <summary>
    /// Source path in the configured backend.
    /// </summary>
    public string Path { get; }

    /// <summary>
    /// Options that select and guard this source.
    /// </summary>
    public ComposeSourceOptions? Options { get; }

    /// <summary>
    /// Converts a path into a source that composes its current version.
    /// </summary>
    public static implicit operator ComposeSource(string path) => new(path);
}
