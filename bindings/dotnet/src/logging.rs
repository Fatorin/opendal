// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Bridge between OpenDAL's logging layer and a managed .NET callback.
//!
//! Every payload handed to the callback borrows memory owned by this crate and
//! is only valid until the callback returns. Nothing here is heap-allocated on
//! behalf of the caller, so there is no release API to pair with these types.

use std::ffi::c_void;
use std::ptr;
use std::sync::atomic::{AtomicU8, Ordering};

use opendal::raw::{Operation, ServiceInfo};
use opendal::{Error, ErrorKind};

use crate::error::{ErrorCode, OpenDALError};
use crate::result::OpendalOperatorResult;
use crate::utils::{require_callback, require_operator};

// Level values shared with the managed side. They match `log::LevelFilter`, so a
// larger value is more verbose and the set already has room for levels this
// bridge does not emit yet. The numeric values are part of the FFI contract and
// must stay stable.
/// Nothing reaches the callback.
pub const LOG_LEVEL_OFF: u8 = 0;
/// Emitted for unexpected errors.
pub const LOG_LEVEL_ERROR: u8 = 1;
/// Emitted for expected errors, such as a missing key.
pub const LOG_LEVEL_WARN: u8 = 2;
/// Reserved; no event is reported at this level today.
pub const LOG_LEVEL_INFO: u8 = 3;
/// Emitted for events that carry no error.
pub const LOG_LEVEL_DEBUG: u8 = 4;
/// Reserved; no event is reported at this level today.
pub const LOG_LEVEL_TRACE: u8 = 5;

/// Coarse gate checked before any marshalling work.
///
/// This mirrors how the `log` crate guards its macros with `max_level()`: one
/// relaxed load rejects the event before any pointer is assembled. Fine-grained
/// filtering is the managed side's job.
static MIN_LEVEL: AtomicU8 = AtomicU8::new(LOG_LEVEL_DEBUG);

/// Borrowed UTF-8 slice.
///
/// Valid only for the duration of the callback that received it.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct OpendalStrRef {
    pub ptr: *const u8,
    pub len: usize,
}

impl OpendalStrRef {
    const EMPTY: Self = Self {
        ptr: ptr::null(),
        len: 0,
    };

    fn new(value: &str) -> Self {
        Self {
            ptr: value.as_ptr(),
            len: value.len(),
        }
    }
}

/// One borrowed key/value pair of log context.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct OpendalLogPairRef {
    pub key: OpendalStrRef,
    pub value: OpendalStrRef,
}

impl OpendalLogPairRef {
    const EMPTY: Self = Self {
        key: OpendalStrRef::EMPTY,
        value: OpendalStrRef::EMPTY,
    };

    fn new(key: &str, value: &str) -> Self {
        Self {
            key: OpendalStrRef::new(key),
            value: OpendalStrRef::new(value),
        }
    }
}

/// Borrowed view of a single log event.
///
/// The error of a failed operation is carried inline rather than through a
/// separate struct. Unlike [`crate::error::OpenDALError`], which owns a heap
/// message released through `opendal_error_release`, everything here borrows and
/// must never be released.
#[repr(C)]
pub struct OpendalLogEventRef {
    pub level: u8,
    /// Discriminant from [`operation_id`], or `u8::MAX` when unrecognized.
    pub operation: u8,
    /// `1` when the operation failed, otherwise `0`.
    pub has_error: u8,
    /// Meaningful only when `has_error` is `1`.
    pub error_code: i32,
    pub scheme: OpendalStrRef,
    /// Bucket, container, or similar. Empty for services that have no such name.
    pub name: OpendalStrRef,
    /// Root the operator was configured with.
    pub root: OpendalStrRef,
    pub message: OpendalStrRef,
    /// Empty when `has_error` is `0`.
    pub error_message: OpendalStrRef,
    pub pair_count: usize,
    pub pairs: *const OpendalLogPairRef,
}

/// Managed callback invoked once per log event that passes the level gate.
pub type LogCallback = extern "C" fn(*const OpendalLogEventRef);

/// Context slices longer than this fall back to a heap buffer. The layer passes
/// at most four pairs today, so that path is a safety net rather than a cost.
const INLINE_PAIRS: usize = 16;

/// Map an operation to a stable discriminant for the managed side.
///
/// [`Operation`] is `#[non_exhaustive]`, so unknown variants degrade to
/// `u8::MAX` instead of failing to compile when core adds one.
fn operation_id(operation: Operation) -> u8 {
    match operation {
        Operation::Info => 0,
        Operation::CreateDir => 1,
        Operation::Read => 2,
        Operation::Write => 3,
        Operation::Copy => 4,
        Operation::Rename => 5,
        Operation::Stat => 6,
        Operation::Delete => 7,
        Operation::List => 8,
        Operation::Presign => 9,
        _ => u8::MAX,
    }
}

/// Derive the event level using the same rules as the default interceptor.
fn level_of(err: Option<&Error>) -> u8 {
    match err {
        Some(err) if err.kind() == ErrorKind::Unexpected => LOG_LEVEL_ERROR,
        Some(_) => LOG_LEVEL_WARN,
        None => LOG_LEVEL_DEBUG,
    }
}

#[derive(Debug, Clone, Copy)]
struct DotnetLoggingInterceptor {
    callback: LogCallback,
}

impl opendal::layers::LoggingInterceptor for DotnetLoggingInterceptor {
    fn log(
        &self,
        info: &ServiceInfo,
        operation: Operation,
        context: &[(&str, &str)],
        message: &str,
        err: Option<&Error>,
    ) {
        // Larger is more verbose, so an event passes when it is at or below the
        // gate, the same way `log` compares against `max_level()`.
        let level = level_of(err);
        if level > MIN_LEVEL.load(Ordering::Relaxed) {
            return;
        }

        let mut inline_pairs = [OpendalLogPairRef::EMPTY; INLINE_PAIRS];
        let mut heap_pairs: Vec<OpendalLogPairRef> = Vec::new();
        let pairs: &[OpendalLogPairRef] = if context.len() <= INLINE_PAIRS {
            for (slot, (key, value)) in inline_pairs.iter_mut().zip(context.iter()) {
                *slot = OpendalLogPairRef::new(key, value);
            }
            &inline_pairs[..context.len()]
        } else {
            heap_pairs.extend(
                context
                    .iter()
                    .map(|(key, value)| OpendalLogPairRef::new(key, value)),
            );
            &heap_pairs
        };

        // The string must outlive the callback, so it is declared here and only
        // initialized on the branch that borrows from it.
        let error_message;
        let (has_error, error_code, error_message_ref) = match err {
            Some(err) => {
                error_message = err.to_string();
                (
                    1u8,
                    ErrorCode::from_error_kind(err.kind()) as i32,
                    OpendalStrRef::new(&error_message),
                )
            }
            None => (0u8, 0, OpendalStrRef::EMPTY),
        };

        // Both hand back an owned Arc, so keep them alive across the call.
        let name = info.name();
        let root = info.root();
        let event = OpendalLogEventRef {
            level,
            operation: operation_id(operation),
            has_error,
            error_code,
            scheme: OpendalStrRef::new(info.scheme()),
            name: OpendalStrRef::new(&name),
            root: OpendalStrRef::new(&root),
            message: OpendalStrRef::new(message),
            error_message: error_message_ref,
            pair_count: pairs.len(),
            pairs: pairs.as_ptr(),
        };

        (self.callback)(&event);
    }
}

/// Set the gate above which events are dropped without marshalling.
///
/// Accepts the `LOG_LEVEL_*` values; anything larger is clamped to
/// [`LOG_LEVEL_TRACE`], the most verbose valid setting. Safe to call from any
/// thread, and takes effect on the next event.
#[unsafe(no_mangle)]
pub extern "C" fn logging_set_min_level(level: u8) {
    MIN_LEVEL.store(level.min(LOG_LEVEL_TRACE), Ordering::Relaxed);
}

/// Create a new operator layered with logging behavior.
///
/// The current operator is not modified. Returned pointer must be released with
/// `operator_free`.
///
/// `callback` is invoked once per event that passes the level gate. Every
/// pointer it receives is borrowed and dies when the callback returns.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `callback` must remain callable for as long as the returned operator lives.
#[unsafe(no_mangle)]
pub extern "C" fn operator_layer_logging(
    op: *const opendal::Operator,
    callback: Option<LogCallback>,
) -> OpendalOperatorResult {
    match operator_layer_logging_inner(op, callback) {
        Ok(value) => OpendalOperatorResult::ok(value),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

fn operator_layer_logging_inner(
    op: *const opendal::Operator,
    callback: Option<LogCallback>,
) -> Result<*mut c_void, OpenDALError> {
    let op = require_operator(op)?;
    let callback = require_callback(callback)?;
    let layer = opendal::layers::LoggingLayer::new(DotnetLoggingInterceptor { callback });

    Ok(Box::into_raw(Box::new(op.clone().layer(layer))) as *mut c_void)
}
