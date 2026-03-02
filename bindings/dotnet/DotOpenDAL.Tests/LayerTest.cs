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

using DotOpenDAL.Layer;

namespace DotOpenDAL.Tests;

public class LayerTest
{
    [Fact]
    public void WithConcurrentLimit_ReplacesCurrentOperator()
    {
        using var op = new Operator("memory");
        var before = op.Op;
        var layered = op.WithLayer(new ConcurrentLimitLayer(4));

        Assert.NotEqual(IntPtr.Zero, layered.Op);
        Assert.Same(op, layered);
        Assert.NotEqual(before, op.Op);

        op.Write("layer-concurrent", [1, 2, 3]);
        var value = op.Read("layer-concurrent");
        Assert.Equal([1, 2, 3], value);
    }

    [Fact]
    public void WithRetry_ReplacesCurrentOperator()
    {
        using var op = new Operator("memory");
        var before = op.Op;
        var layered = op.WithLayer(new RetryLayer
        {
            Jitter = false,
            Factor = 2,
            MinDelay = TimeSpan.FromMilliseconds(1),
            MaxDelay = TimeSpan.FromMilliseconds(10),
            MaxTimes = 2,
        });

        Assert.NotEqual(IntPtr.Zero, layered.Op);
        Assert.Same(op, layered);
        Assert.NotEqual(before, op.Op);

        op.Write("layer-retry", [4, 5, 6]);
        var value = op.Read("layer-retry");
        Assert.Equal([4, 5, 6], value);
    }

    [Fact]
    public void WithConcurrentLimit_ZeroPermits_ThrowsArgumentOutOfRangeException()
    {
        using var op = new Operator("memory");

        Assert.Throws<ArgumentOutOfRangeException>(() => op.WithLayer(new ConcurrentLimitLayer(0)));
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
    public void WithTimeout_ReplacesCurrentOperator()
    {
        using var op = new Operator("memory");
        var before = op.Op;
        var layered = op.WithLayer(new TimeoutLayer
        {
            Timeout = TimeSpan.FromSeconds(5),
            IoTimeout = TimeSpan.FromSeconds(2),
        });

        Assert.NotEqual(IntPtr.Zero, layered.Op);
        Assert.Same(op, layered);
        Assert.NotEqual(before, op.Op);

        op.Write("layer-timeout", [7, 8, 9]);
        var value = op.Read("layer-timeout");
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
}
