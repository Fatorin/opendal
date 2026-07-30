---
title: Going to production
sidebar_label: Going to production
description: Make OpenDAL robust in .NET — layers, error handling, capability checks, and executor lifetime.
---

# Going to production

The basics read and write data. Production code also has to survive transient
failures, bound its resource use, handle errors precisely, and manage native
resource lifetime.

## Layers

A layer wraps an operator to add cross-cutting behavior without touching your
storage code. `WithLayer(...)` returns a new operator with the layer applied;
chain calls to compose several:

```csharp
using OpenDAL;
using OpenDAL.Layer;

using var baseOp = new Operator("memory");

using var op = baseOp
    .WithLayer(new ConcurrentLimitLayer(64))
    .WithLayer(new RetryLayer
    {
        MaxTimes = 5,
        MinDelay = TimeSpan.FromMilliseconds(100),
        MaxDelay = TimeSpan.FromSeconds(5),
        Factor = 2f,
        Jitter = true,
    })
    .WithLayer(new TimeoutLayer
    {
        Timeout = TimeSpan.FromSeconds(30),
        IoTimeout = TimeSpan.FromSeconds(5),
    });
```

The .NET binding exposes these layers in `OpenDAL.Layer`:

| Layer | What it does |
|-------|--------------|
| `RetryLayer` | Retries transient failures with exponential backoff (`MaxTimes`, `MinDelay`, `MaxDelay`, `Factor`, `Jitter`). |
| `TimeoutLayer` | Bounds slow calls with a total `Timeout` and a per-I/O `IoTimeout`. |
| `ConcurrentLimitLayer` | Caps concurrent operations, with an optional second argument for concurrent HTTP requests. |
| `ThrottleLayer` | Rate-limits the byte flow of reads and writes (`bandwidth` per second, `burst` at once). |
| `LoggingLayer` | Reports every operation to `OpenDALLogging.Handler`. See [Logging](#logging). |
| `MimeGuessLayer` | Fills in `Content-Type` from the path extension when nothing else set one. |
| `CapabilityOverrideLayer` | Overrides reported capabilities. |

See [Concepts](../../03-concepts.mdx#layer) for the model.

## Logging

`LoggingLayer` reports each operation to a process-wide handler. Set the handler
and the level before applying the layer:

```csharp
using OpenDAL.Layer;
using OpenDAL.Logging;

OpenDALLogging.MinimumLevel = OpenDALLogLevel.Debug;
OpenDALLogging.Handler = (in OpenDALLogEvent evt) =>
{
    Console.WriteLine(Encoding.UTF8.GetString(evt.Message));
};

using var op = baseOp.WithLayer(new LoggingLayer());
```

The event is a `ref struct` over native memory and is valid only until the handler
returns. The compiler rejects storing it, capturing it in a lambda, or carrying it
across an `await`, so copy out what you need first.

Asynchronous operations report from native worker threads, so the handler runs
concurrently and must be thread-safe. Exceptions it throws are swallowed, because
letting one reach native code would abort the process.

### Bridging to Microsoft.Extensions.Logging

The binding does not depend on a logging framework. This handler forwards events
to `ILogger`, which covers Serilog, NLog, and log4net:

```csharp
var loggers = new ConcurrentDictionary<string, ILogger>();

OpenDALLogging.Handler = (in OpenDALLogEvent evt) =>
{
    var scheme = Encoding.UTF8.GetString(evt.Scheme);
    var logger = loggers.GetOrAdd(scheme, s => loggerFactory.CreateLogger($"OpenDAL.{s}"));

    var level = evt.Level switch
    {
        OpenDALLogLevel.Error => LogLevel.Error,
        OpenDALLogLevel.Warning => LogLevel.Warning,
        OpenDALLogLevel.Information => LogLevel.Information,
        OpenDALLogLevel.Trace => LogLevel.Trace,
        _ => LogLevel.Debug,
    };

    if (!logger.IsEnabled(level))
    {
        return;
    }

    // Passing the context as key/value pairs rather than a formatted string lets
    // structured providers index it.
    var state = new List<KeyValuePair<string, object?>>
    {
        new("operation", evt.Operation.ToString()),
        new("event", Encoding.UTF8.GetString(evt.Message)),
    };

    for (var i = 0; i < evt.ContextCount; i++)
    {
        state.Add(new KeyValuePair<string, object?>(
            Encoding.UTF8.GetString(evt.GetContextKey(i)),
            Encoding.UTF8.GetString(evt.GetContextValue(i))));
    }

    Exception? error = null;
    if (evt.TryGetError(out var code, out var errorMessage))
    {
        error = new OpenDALException(code, Encoding.UTF8.GetString(errorMessage));
    }

    logger.Log(level, default, state, error, static (s, _) => "OpenDAL");
};
```

The error goes in the `exception` argument so structured sinks can group failures
by it. `OpenDALException.Code` matches the code a failed call throws.

### Level filtering

`MinimumLevel` defaults to `Debug`, so leaving it alone loses no events. They then
cross into managed code and the logging framework's filter discards them, after
the handler has already decoded at least the scheme. Setting `MinimumLevel` to
match the framework's level avoids that work.

The native side caches the level so it can reject events without calling into
managed code, which also means it does not see an `appsettings.json` reload or a
Serilog level switch. Assign `MinimumLevel` again from that notification.

### Layer order

Apply the layer last so it sits outermost. Nesting it inside `RetryLayer` reports
each retry attempt as its own event, because retries happen inside that layer.
`RetryLayer` reports attempts through its own interceptor.

## Error handling

Most failures surface as `OpenDALException` with a typed `Code` of type
`ErrorCode`. Match on the code instead of inspecting messages:

```csharp
using OpenDAL;

try
{
    await op.ReadAsync("maybe-missing.txt");
}
catch (OpenDALException ex) when (ex.Code == ErrorCode.NotFound)
{
    // handle the missing object
}
```

Common codes include `NotFound`, `PermissionDenied`, `AlreadyExists`,
`ConditionNotMatch`, `RateLimited`, and `Unsupported`. Unknown native codes are
normalized to `ErrorCode.Unexpected`.

## Capability checks {#capability-checks}

Not every service supports every operation. Query what a backend can do through
`op.Info.Capability` before calling optional operations like `Copy`, `Rename`,
or presign:

```csharp
var cap = op.Info.Capability;
if (cap.PresignRead)
{
    var req = await op.PresignReadAsync("a.txt", TimeSpan.FromMinutes(5));
}
```

Calling an unsupported operation throws `OpenDALException` with
`ErrorCode.Unsupported`, so capability checks are an optimization, not a
requirement for safety. `Info` also exposes the operator's `Scheme`, `Root`, and
`Name`.

## Executor and lifetime {#executor-and-lifetime}

The binding wraps native handles, so deterministic disposal matters:

- Prefer `using` for `Operator`, `Executor`, and stream instances.
- Keep an `Executor` alive for the full lifetime of the operations using it.
- Disposing an `Executor` or `Operator` too early throws
  `ObjectDisposedException`. For async calls, ensure disposal happens **after**
  the awaited operations complete.
- If you do not pass an `Executor`, OpenDAL uses a shared default executor.

See [Getting started — Executors](./02-getting-started.md#executors) for how to
create and pass an executor.

## Path conventions

- Use backend-native object keys, for example `a/b/c.txt`.
- For directory-like operations (`CreateDir`, directory `Stat`, listing a
  directory root) prefer trailing-slash paths such as `logs/`.
