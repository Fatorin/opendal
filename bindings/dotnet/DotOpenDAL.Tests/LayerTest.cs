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
    public void WithConcurrentLimit_ReturnsNewOperator()
    {
        using var op = new Operator("memory");
        using var layered = op.WithConcurrentLimit(4);

        Assert.NotEqual(IntPtr.Zero, layered.Op);
        Assert.NotEqual(op.Op, layered.Op);

        layered.Write("layer-concurrent", [1, 2, 3]);
        var value = layered.Read("layer-concurrent");
        Assert.Equal([1, 2, 3], value);
    }

    [Fact]
    public void WithRetry_ReturnsNewOperator()
    {
        using var op = new Operator("memory");
        using var layered = op.WithRetry(
            jitter: false,
            factor: 2,
            minDelay: TimeSpan.FromMilliseconds(1),
            maxDelay: TimeSpan.FromMilliseconds(10),
            maxTimes: 2);

        Assert.NotEqual(IntPtr.Zero, layered.Op);
        Assert.NotEqual(op.Op, layered.Op);

        layered.Write("layer-retry", [4, 5, 6]);
        var value = layered.Read("layer-retry");
        Assert.Equal([4, 5, 6], value);
    }

    [Fact]
    public void WithConcurrentLimit_ZeroPermits_ThrowsArgumentOutOfRangeException()
    {
        using var op = new Operator("memory");

        Assert.Throws<ArgumentOutOfRangeException>(() => op.WithConcurrentLimit(0));
    }

    [Fact]
    public void WithRetry_InvalidFactor_ThrowsArgumentOutOfRangeException()
    {
        using var op = new Operator("memory");

        Assert.Throws<ArgumentOutOfRangeException>(() => op.WithRetry(factor: 0));
    }

    [Fact]
    public void WithTimeout_ReturnsNewOperator()
    {
        using var op = new Operator("memory");
        using var layered = op.WithTimeout(
            timeout: TimeSpan.FromSeconds(5),
            ioTimeout: TimeSpan.FromSeconds(2));

        Assert.NotEqual(IntPtr.Zero, layered.Op);
        Assert.NotEqual(op.Op, layered.Op);

        layered.Write("layer-timeout", [7, 8, 9]);
        var value = layered.Read("layer-timeout");
        Assert.Equal([7, 8, 9], value);
    }

    [Fact]
    public void WithTimeout_ZeroTimeout_ThrowsArgumentOutOfRangeException()
    {
        using var op = new Operator("memory");

        Assert.Throws<ArgumentOutOfRangeException>(() => op.WithTimeout(timeout: TimeSpan.Zero));
    }
}
