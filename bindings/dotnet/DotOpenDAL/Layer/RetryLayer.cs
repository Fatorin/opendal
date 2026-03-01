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

public sealed class RetryLayer : ILayer
{
    public bool Jitter { get; init; }

    public float Factor { get; init; } = 2f;

    public TimeSpan MinDelay { get; init; } = TimeSpan.FromSeconds(1);

    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(60);

    public nuint MaxTimes { get; init; } = 3;

    public Operator Apply(Operator op)
    {
        ArgumentNullException.ThrowIfNull(op);
        ObjectDisposedException.ThrowIf(op.IsInvalid, op);
        Validate();

        var result = NativeMethods.operator_layer_retry(
            op,
            Jitter,
            Factor,
            ToNanos(MinDelay),
            ToNanos(MaxDelay),
            MaxTimes);

        return Operator.FromLayerResult(result);
    }

    private void Validate()
    {
        if (float.IsNaN(Factor) || float.IsInfinity(Factor) || Factor <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(Factor), "Factor must be a positive finite number.");
        }

        if (MinDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(MinDelay), "MinDelay must be non-negative.");
        }

        if (MaxDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxDelay), "MaxDelay must be non-negative.");
        }

        if (MaxDelay < MinDelay)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxDelay), "MaxDelay must be greater than or equal to MinDelay.");
        }
    }

    private static ulong ToNanos(TimeSpan delay)
    {
        return checked((ulong)delay.Ticks * 100UL);
    }
}
