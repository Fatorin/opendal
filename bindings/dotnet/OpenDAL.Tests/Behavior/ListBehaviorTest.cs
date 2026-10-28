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

namespace OpenDAL.Tests;

[Collection("BehaviorOperator")]
public sealed class ListBehaviorTest : BehaviorTestBase
{
    private static CancellationToken CT => TestContext.Current.CancellationToken;

    public ListBehaviorTest(BehaviorOperatorFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public void ListBehavior_Check_Succeeds()
    {
        if (!Supports(c => c.Read && c.Write && c.List))
        {
            return;
        }

        Op.Check();
    }

    [Fact]
    public async Task ListBehavior_Check_SucceedsAsync()
    {
        if (!Supports(c => c.Read && c.Write && c.List))
        {
            return;
        }

        await Op.CheckAsync(CT);
    }

    [Fact]
    public void ListBehavior_ListsEntriesUnderPrefix()
    {
        if (!Supports(c => c.List && c.Write && c.CreateDir))
        {
            return;
        }

        var dir = NewPath("list") + "/";
        var a = $"{dir}a.txt";
        var b = $"{dir}nested/b.txt";

        Op.CreateDir(dir);
        Op.Write(a, RandomBytes(10));
        Op.Write(b, RandomBytes(20));

        var entries = Op.List(dir, new ListOptions { Recursive = true });

        Assert.Contains(entries, e => e.Path.EndsWith("a.txt", StringComparison.Ordinal));
        Assert.Contains(entries, e => e.Path.EndsWith("nested/b.txt", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ListBehavior_ListsEntriesUnderPrefixAsync()
    {
        if (!Supports(c => c.List && c.Write && c.CreateDir))
        {
            return;
        }

        var dir = NewPath("list-async") + "/";
        var a = $"{dir}a.txt";
        var b = $"{dir}nested/b.txt";

        await Op.CreateDirAsync(dir, CT);
        await Op.WriteAsync(a, RandomBytes(10), cancellationToken: CT);
        await Op.WriteAsync(b, RandomBytes(20), cancellationToken: CT);

        var entries = await Op.ListAsync(dir, new ListOptions { Recursive = true }, CT);

        Assert.Contains(entries, e => e.Path.EndsWith("a.txt", StringComparison.Ordinal));
        Assert.Contains(entries, e => e.Path.EndsWith("nested/b.txt", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ListBehavior_WithLimit_ReturnsAtMostRequestedSizeAsync()
    {
        if (!Supports(c => c.List && c.ListWithLimit && c.Write))
        {
            return;
        }

        var dir = NewPath("list-limit") + "/";
        var files = new List<string>();
        for (var i = 0; i < 6; i++)
        {
            var path = $"{dir}file-{i}.txt";
            files.Add(path);
            await Op.WriteAsync(path, RandomBytes(8), cancellationToken: CT);
        }

        var entries = await Op.ListAsync(dir, new ListOptions { Recursive = true, Limit = 3 }, CT);
        foreach (var file in files)
        {
            Assert.Contains(entries, e => e.Path == file);
        }
    }

    [Fact]
    public void ListBehavior_WithLimit_ReturnsAtMostRequestedSize()
    {
        if (!Supports(c => c.List && c.ListWithLimit && c.Write))
        {
            return;
        }

        var dir = NewPath("list-limit-sync") + "/";
        var files = new List<string>();
        for (var i = 0; i < 6; i++)
        {
            var path = $"{dir}file-{i}.txt";
            files.Add(path);
            Op.Write(path, RandomBytes(8));
        }

        var entries = Op.List(dir, new ListOptions { Recursive = true, Limit = 3 });
        foreach (var file in files)
        {
            Assert.Contains(entries, e => e.Path == file);
        }
    }

    [Fact]
    public void ListBehavior_WithVersions_ReportsCurrentVersion()
    {
        if (!Supports(c => c.List && c.ListWithVersions && c.Write))
        {
            return;
        }

        var dir = NewPath("list-versions") + "/";
        var path = $"{dir}file.txt";

        Op.Write(path, RandomBytes(8));
        Op.Write(path, RandomBytes(16));

        var versions = Op.List(dir, new ListOptions { Versions = true })
            .Where(e => e.Path == path)
            .ToList();

        Assert.True(versions.Count >= 2, $"expected at least two versions, got {versions.Count}");
        Assert.Single(versions, e => e.Metadata.IsCurrent == true);
    }

    [Fact]
    public void ListBehavior_WithDeleted_ReportsDeleteMarker()
    {
        if (!Supports(c => c.List && c.ListWithDeleted && c.Write && c.Delete))
        {
            return;
        }

        var dir = NewPath("list-deleted") + "/";
        var path = $"{dir}file.txt";

        Op.Write(path, RandomBytes(8));
        Op.Delete(path);

        var entries = Op.List(dir, new ListOptions { Deleted = true });

        Assert.Contains(entries, e => e.Path == path && e.Metadata.IsDeleted);
    }

    [Fact]
    public void ListBehavior_EnumerateEntries_CrossesBatchBoundaries()
    {
        if (!Supports(c => c.List && c.Write && c.CreateDir))
        {
            return;
        }

        // More files than one native batch holds, so enumeration must fetch
        // several batches.
        var dir = NewPath("enumerate") + "/";
        Op.CreateDir(dir);
        const int fileCount = 300;
        for (var i = 0; i < fileCount; i++)
        {
            Op.Write($"{dir}file-{i:D3}", RandomBytes(1));
        }

        var files = Op.EnumerateEntries(dir)
            .Where(e => e.Metadata.IsFile)
            .Select(e => e.Path)
            .ToList();

        Assert.Equal(fileCount, files.Count);
        Assert.Equal(
            Op.List(dir).Where(e => e.Metadata.IsFile).Select(e => e.Path).Order(StringComparer.Ordinal),
            files.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task ListBehavior_EnumerateEntriesAsync_ListsRecursively()
    {
        if (!Supports(c => c.List && c.Write && c.CreateDir))
        {
            return;
        }

        var dir = NewPath("enumerate-async") + "/";
        await Op.CreateDirAsync(dir, CT);
        await Op.WriteAsync($"{dir}a.txt", RandomBytes(10), cancellationToken: CT);
        await Op.WriteAsync($"{dir}nested/b.txt", RandomBytes(20), cancellationToken: CT);

        var entries = new List<Entry>();
        await foreach (var entry in Op.EnumerateEntriesAsync(dir, new ListOptions { Recursive = true }, CT))
        {
            entries.Add(entry);
        }

        Assert.Contains(entries, e => e.Path.EndsWith("a.txt", StringComparison.Ordinal));
        Assert.Contains(entries, e => e.Path.EndsWith("nested/b.txt", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ListBehavior_EnumerateEntries_StopsEarlyAndRestartsAsync()
    {
        if (!Supports(c => c.List && c.Write && c.CreateDir))
        {
            return;
        }

        var dir = NewPath("enumerate-early") + "/";
        await Op.CreateDirAsync(dir, CT);
        await Op.WriteAsync($"{dir}a.txt", RandomBytes(1), cancellationToken: CT);
        await Op.WriteAsync($"{dir}b.txt", RandomBytes(1), cancellationToken: CT);

        var sequence = Op.EnumerateEntriesAsync(dir, cancellationToken: CT);
        await foreach (var _ in sequence)
        {
            break;
        }

        // A new enumeration starts a fresh listing.
        var count = 0;
        await foreach (var entry in sequence)
        {
            if (entry.Metadata.IsFile)
            {
                count++;
            }
        }

        Assert.Equal(2, count);
        Assert.Equal(2, Op.EnumerateEntries(dir).Count(e => e.Metadata.IsFile));
    }

    [Fact]
    public async Task ListBehavior_EnumerateEntriesAsync_HonorsCancellation()
    {
        if (!Supports(c => c.List && c.Write && c.CreateDir))
        {
            return;
        }

        var dir = NewPath("enumerate-cancel") + "/";
        await Op.CreateDirAsync(dir, CT);
        await Op.WriteAsync($"{dir}a.txt", RandomBytes(1), cancellationToken: CT);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in Op.EnumerateEntriesAsync(dir, cancellationToken: cts.Token))
            {
            }
        });
    }
}
