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

using DotOpenDAL.Layer.Abstractions;

namespace DotOpenDAL.Layer;

public sealed class ConcurrentLimitLayer : ILayer
{
    public nuint Permits { get; }

    public ConcurrentLimitLayer(nuint permits)
    {
        if (permits == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(permits), "Permits must be greater than zero.");
        }
        Permits = permits;
    }

    public Operator Apply(Operator op)
    {
        ArgumentNullException.ThrowIfNull(op);
        ObjectDisposedException.ThrowIf(op.IsInvalid, op);

        var result = NativeMethods.operator_layer_concurrent_limit(op, Permits);
        return Operator.FromLayerResult(result);
    }
}
