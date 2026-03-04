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

use crate::{
    byte_buffer::ByteBuffer,
    entry::into_entry_list_ptr,
    executor::executor_or_default,
    error::OpenDALError,
    metadata::OpendalMetadata,
    options::{
        OpendalConstructorOptions, parse_list_options, parse_read_options, parse_stat_options,
        parse_write_options,
    },
    operator_info::OpendalOperatorInfo,
    result::{
        OpendalEntryListResult, OpendalMetadataResult, OpendalReadResult,
        OpendalOperatorInfoResult, OpendalOperatorResult, OpendalOptionsResult, OpendalResult,
    },
    utils::{
        collect_options, into_operator_info, require_callback, require_cstr, require_data_ptr,
        require_operator,
    },
    validators::prelude::{
        validate_concurrent_limit_options, validate_retry_options, validate_timeout_options,
    },
};

use std::ffi::c_void;
use std::os::raw::c_char;
use std::time::Duration;

/// Callback signature for async write completion.
///
/// The callback is provided by the .NET side and must remain valid until
/// invoked by Rust.
type WriteCallback = unsafe extern "C" fn(context: i64, result: OpendalResult);
/// Callback signature for async read completion.
type ReadCallback = unsafe extern "C" fn(context: i64, result: OpendalReadResult);
/// Callback signature for async stat completion.
type StatCallback = unsafe extern "C" fn(context: i64, result: OpendalMetadataResult);
/// Callback signature for async list completion.
type ListCallback = unsafe extern "C" fn(context: i64, result: OpendalEntryListResult);

/// Build constructor options from raw C string key/value arrays.
///
/// On success, the returned pointer must be released by
/// `constructor_option_free`.
/// # Safety
///
/// - When `len > 0`, `keys` and `values` must be non-null pointers to arrays
///   containing at least `len` C-string pointers.
/// - Each key/value entry must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn constructor_option_build(
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
) -> OpendalOptionsResult {
    match unsafe { collect_options(keys, values, len) } {
        Ok(values) => {
            let options = OpendalConstructorOptions::from_values(values);
            OpendalOptionsResult::ok(Box::into_raw(Box::new(options)) as *mut c_void)
        }
        Err(error) => OpendalOptionsResult::from_error(error),
    }
}

/// Release constructor options created by `constructor_option_build`.
/// # Safety
///
/// - `options` must be null or a pointer returned by
///   `constructor_option_build`.
/// - This function must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn constructor_option_free(options: *mut OpendalConstructorOptions) {
    if options.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(options));
    }
}

/// Build read options from raw C string key/value arrays.
///
/// On success, the returned pointer must be released by `read_option_free`.
/// # Safety
///
/// - When `len > 0`, `keys` and `values` must be non-null pointers to arrays
///   containing at least `len` C-string pointers.
/// - Each key/value entry must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn read_option_build(
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
) -> OpendalOptionsResult {
    match unsafe { collect_options(keys, values, len) }
        .and_then(|values| parse_read_options(&values))
    {
        Ok(options) => OpendalOptionsResult::ok(Box::into_raw(Box::new(options)) as *mut c_void),
        Err(error) => OpendalOptionsResult::from_error(error),
    }
}

/// Release read options created by `read_option_build`.
/// # Safety
///
/// - `options` must be null or a pointer returned by `read_option_build`.
/// - This function must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn read_option_free(options: *mut opendal::options::ReadOptions) {
    if options.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(options));
    }
}

/// Build write options from raw C string key/value arrays.
///
/// On success, the returned pointer must be released by `write_option_free`.
/// # Safety
///
/// - When `len > 0`, `keys` and `values` must be non-null pointers to arrays
///   containing at least `len` C-string pointers.
/// - Each key/value entry must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn write_option_build(
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
) -> OpendalOptionsResult {
    match unsafe { collect_options(keys, values, len) }
        .and_then(|values| parse_write_options(&values))
    {
        Ok(options) => OpendalOptionsResult::ok(Box::into_raw(Box::new(options)) as *mut c_void),
        Err(error) => OpendalOptionsResult::from_error(error),
    }
}

/// Release write options created by `write_option_build`.
/// # Safety
///
/// - `options` must be null or a pointer returned by `write_option_build`.
/// - This function must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn write_option_free(options: *mut opendal::options::WriteOptions) {
    if options.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(options));
    }
}

/// Build stat options from raw C string key/value arrays.
///
/// On success, the returned pointer must be released by `stat_option_free`.
/// # Safety
///
/// - When `len > 0`, `keys` and `values` must be non-null pointers to arrays
///   containing at least `len` C-string pointers.
/// - Each key/value entry must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stat_option_build(
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
) -> OpendalOptionsResult {
    match unsafe { collect_options(keys, values, len) }
        .and_then(|values| parse_stat_options(&values))
    {
        Ok(options) => OpendalOptionsResult::ok(Box::into_raw(Box::new(options)) as *mut c_void),
        Err(error) => OpendalOptionsResult::from_error(error),
    }
}

/// Release stat options created by `stat_option_build`.
/// # Safety
///
/// - `options` must be null or a pointer returned by `stat_option_build`.
/// - This function must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stat_option_free(options: *mut opendal::options::StatOptions) {
    if options.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(options));
    }
}

/// Build list options from raw C string key/value arrays.
///
/// On success, the returned pointer must be released by `list_option_free`.
/// # Safety
///
/// - When `len > 0`, `keys` and `values` must be non-null pointers to arrays
///   containing at least `len` C-string pointers.
/// - Each key/value entry must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn list_option_build(
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
) -> OpendalOptionsResult {
    match unsafe { collect_options(keys, values, len) }
        .and_then(|values| parse_list_options(&values))
    {
        Ok(options) => OpendalOptionsResult::ok(Box::into_raw(Box::new(options)) as *mut c_void),
        Err(error) => OpendalOptionsResult::from_error(error),
    }
}

/// Release list options created by `list_option_build`.
/// # Safety
///
/// - `options` must be null or a pointer returned by `list_option_build`.
/// - This function must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn list_option_free(options: *mut opendal::options::ListOptions) {
    if options.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(options));
    }
}

/// Construct an OpenDAL operator instance from a scheme and key/value options.
///
/// Returns a pointer that must be released with `operator_free`.
/// # Safety
///
/// - `scheme` must be a valid null-terminated UTF-8 string.
/// - When `len > 0`, `keys` and `values` must be non-null and point to arrays
///   of at least `len` entries.
/// - Every key/value entry in those arrays must be a valid null-terminated
///   UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_construct(
    scheme: *const c_char,
    options: *const OpendalConstructorOptions,
) -> OpendalOperatorResult {
    match operator_construct_inner(scheme, options) {
        Ok(op) => OpendalOperatorResult::ok(op),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

fn operator_construct_inner(
    scheme: *const c_char,
    options: *const OpendalConstructorOptions,
) -> Result<*mut c_void, OpenDALError> {
    let scheme = require_cstr(scheme, "scheme")?;
    let options = if options.is_null() {
        OpendalConstructorOptions::default().into_values()
    } else {
        unsafe { (&*options).clone().into_values() }
    };
    let op =
        opendal::Operator::via_iter(scheme, options).map_err(OpenDALError::from_opendal_error)?;
    Ok(Box::into_raw(Box::new(op)) as *mut c_void)
}

/// Release an operator created by `operator_construct`.
/// # Safety
///
/// - `op` must be either null or a pointer returned by `operator_construct`.
/// - The pointer must not be used after this call.
/// - This function must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_free(op: *mut opendal::Operator) {
    if op.is_null() {
        return;
    }

    unsafe {
        drop(Box::from_raw(op));
    }
}

/// Get operator info payload.
///
/// On success, payload must be released by `opendal_operator_info_result_release`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_info_get(
    op: *const opendal::Operator,
) -> OpendalOperatorInfoResult {
    match operator_info_get_inner(op) {
        Ok(value) => OpendalOperatorInfoResult::ok(value),
        Err(error) => OpendalOperatorInfoResult::from_error(error),
    }
}

fn operator_info_get_inner(
    op: *const opendal::Operator,
) -> Result<*mut c_void, OpenDALError> {
    let op = require_operator(op)?;
    let info = into_operator_info(op.info());
    Ok(Box::into_raw(Box::new(info)) as *mut c_void)
}

/// Release an operator info payload created by `operator_info_get`.
/// # Safety
///
/// - `info` must be either null or a pointer returned by `operator_info_get`.
/// - The pointer must not be used after this call.
/// - This function must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_info_free(info: *mut OpendalOperatorInfo) {
    if info.is_null() {
        return;
    }

    unsafe {
        let info = Box::from_raw(info);
        if !info.scheme.is_null() {
            drop(std::ffi::CString::from_raw(info.scheme));
        }
        if !info.root.is_null() {
            drop(std::ffi::CString::from_raw(info.root));
        }
        if !info.name.is_null() {
            drop(std::ffi::CString::from_raw(info.name));
        }
    }
}

/// Create a new operator layered with retry behavior.
///
/// The current operator is not modified. Returned pointer must be released with
/// `operator_free`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_layer_retry(
    op: *const opendal::Operator,
    jitter: bool,
    factor: f32,
    min_delay_nanos: u64,
    max_delay_nanos: u64,
    max_times: usize,
) -> OpendalOperatorResult {
    match operator_layer_retry_inner(op, jitter, factor, min_delay_nanos, max_delay_nanos, max_times) {
        Ok(value) => OpendalOperatorResult::ok(value),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

fn operator_layer_retry_inner(
    op: *const opendal::Operator,
    jitter: bool,
    factor: f32,
    min_delay_nanos: u64,
    max_delay_nanos: u64,
    max_times: usize,
) -> Result<*mut c_void, OpenDALError> {
    let op = require_operator(op)?;
    validate_retry_options(factor, min_delay_nanos, max_delay_nanos)?;

    let mut retry = opendal::layers::RetryLayer::new();
    retry = retry.with_factor(factor);
    retry = retry.with_min_delay(Duration::from_nanos(min_delay_nanos));
    retry = retry.with_max_delay(Duration::from_nanos(max_delay_nanos));
    retry = retry.with_max_times(max_times);
    if jitter {
        retry = retry.with_jitter();
    }

    Ok(Box::into_raw(Box::new(op.clone().layer(retry))) as *mut c_void)
}

/// Create a new operator layered with timeout behavior.
///
/// The current operator is not modified. Returned pointer must be released with
/// `operator_free`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_layer_timeout(
    op: *const opendal::Operator,
    timeout_nanos: u64,
    io_timeout_nanos: u64,
) -> OpendalOperatorResult {
    match operator_layer_timeout_inner(op, timeout_nanos, io_timeout_nanos) {
        Ok(value) => OpendalOperatorResult::ok(value),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

fn operator_layer_timeout_inner(
    op: *const opendal::Operator,
    timeout_nanos: u64,
    io_timeout_nanos: u64,
) -> Result<*mut c_void, OpenDALError> {
    let op = require_operator(op)?;
    validate_timeout_options(timeout_nanos, io_timeout_nanos)?;

    let timeout = opendal::layers::TimeoutLayer::new()
        .with_timeout(Duration::from_nanos(timeout_nanos))
        .with_io_timeout(Duration::from_nanos(io_timeout_nanos));

    Ok(Box::into_raw(Box::new(op.clone().layer(timeout))) as *mut c_void)
}

/// Create a new operator layered with concurrent-limit behavior.
///
/// The current operator is not modified. Returned pointer must be released with
/// `operator_free`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_layer_concurrent_limit(
    op: *const opendal::Operator,
    permits: usize,
) -> OpendalOperatorResult {
    match operator_layer_concurrent_limit_inner(op, permits) {
        Ok(value) => OpendalOperatorResult::ok(value),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

fn operator_layer_concurrent_limit_inner(
    op: *const opendal::Operator,
    permits: usize,
) -> Result<*mut c_void, OpenDALError> {
    let op = require_operator(op)?;
    validate_concurrent_limit_options(permits)?;

    let concurrent_limit = opendal::layers::ConcurrentLimitLayer::new(permits);
    Ok(Box::into_raw(Box::new(op.clone().layer(concurrent_limit))) as *mut c_void)
}

/// Write bytes to `path` synchronously with options.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `len > 0`, `data` must be non-null and readable for `len` bytes.
/// - When `option_len > 0`, `option_keys` and `option_values` must be valid arrays.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_write_with_options(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    data: *const u8,
    len: usize,
    options: *const opendal::options::WriteOptions,
) -> OpendalResult {
    match operator_write_with_options_inner(op, executor, path, data, len, options) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_write_with_options_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    data: *const u8,
    len: usize,
    options: *const opendal::options::WriteOptions,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = unsafe { executor_or_default(executor) }?;
    let path = require_cstr(path, "path")?;
    require_data_ptr(data, len)?;
    let options = if options.is_null() {
        opendal::options::WriteOptions::default()
    } else {
        unsafe { (&*options).clone() }
    };

    let payload = if len == 0 {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(data, len) }
    };

    executor
        .block_on(op.write_options(path, payload, options))
        .map(|_| ())
        .map_err(OpenDALError::from_opendal_error)
}

/// Write bytes to `path` asynchronously with options.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `len > 0`, `data` must be non-null and readable for `len` bytes.
/// - When `option_len > 0`, `option_keys` and `option_values` must be valid arrays.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_write_with_options_async(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    data: *const u8,
    len: usize,
    options: *const opendal::options::WriteOptions,
    callback: Option<WriteCallback>,
    context: i64,
) -> OpendalResult {
    match operator_write_with_options_async_inner(op, executor, path, data, len, options, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_write_with_options_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    data: *const u8,
    len: usize,
    options: *const opendal::options::WriteOptions,
    callback: Option<WriteCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = unsafe { executor_or_default(executor) }?;
    let path = require_cstr(path, "path")?.to_string();
    require_data_ptr(data, len)?;
    let callback = require_callback(callback)?;
    let options = if options.is_null() {
        opendal::options::WriteOptions::default()
    } else {
        unsafe { (&*options).clone() }
    };

    let payload = if len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(data, len) }.to_vec()
    };

    let op = op.clone();
    executor.spawn(async move {
        let result = op
            .write_options(&path, payload, options)
            .await
            .map(|_| ())
            .map_err(OpenDALError::from_opendal_error);

        unsafe {
            callback(
                context,
                match result {
                    Ok(()) => OpendalResult::ok(),
                    Err(error) => OpendalResult::from_error(error),
                },
            );
        }
    });

    Ok(())
}

/// Read bytes from `path` synchronously with options.
///
/// On success, the returned buffer must be released with `opendal_read_result_release`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `option_len > 0`, `option_keys` and `option_values` must be valid arrays.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_read_with_options(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::ReadOptions,
) -> OpendalReadResult {
    match operator_read_with_options_inner(op, executor, path, options) {
        Ok(value) => OpendalReadResult::ok(value),
        Err(error) => OpendalReadResult::from_error(error),
    }
}

fn operator_read_with_options_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::ReadOptions,
) -> Result<ByteBuffer, OpenDALError> {
    let op = require_operator(op)?;
    let executor = unsafe { executor_or_default(executor) }?;
    let path = require_cstr(path, "path")?;
    let options = if options.is_null() {
        opendal::options::ReadOptions::default()
    } else {
        unsafe { (&*options).clone() }
    };

    let value = executor
        .block_on(op.read_options(path, options))
        .map(|v| v.to_vec())
        .map_err(OpenDALError::from_opendal_error)?;

    Ok(ByteBuffer::from_vec(value))
}

/// Read bytes from `path` asynchronously with options.
///
/// The callback is invoked exactly once. On successful reads, the callback
/// result must be released with `opendal_read_result_release`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `option_len > 0`, `option_keys` and `option_values` must be valid arrays.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_read_with_options_async(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::ReadOptions,
    callback: Option<ReadCallback>,
    context: i64,
) -> OpendalResult {
    match operator_read_with_options_async_inner(op, executor, path, options, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_read_with_options_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::ReadOptions,
    callback: Option<ReadCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = unsafe { executor_or_default(executor) }?;
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let options = if options.is_null() {
        opendal::options::ReadOptions::default()
    } else {
        unsafe { (&*options).clone() }
    };

    let op = op.clone();
    executor.spawn(async move {
        let result = op
            .read_options(&path, options)
            .await
            .map(|v| ByteBuffer::from_vec(v.to_vec()))
            .map_err(OpenDALError::from_opendal_error);

        unsafe {
            callback(
                context,
                match result {
                    Ok(value) => OpendalReadResult::ok(value),
                    Err(error) => OpendalReadResult::from_error(error),
                },
            );
        }
    });

    Ok(())
}

/// Stat `path` synchronously with options.
///
/// On success, returned payload must be released with `opendal_metadata_result_release`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `option_len > 0`, `option_keys` and `option_values` must be valid arrays.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_stat_with_options(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::StatOptions,
) -> OpendalMetadataResult {
    match operator_stat_with_options_inner(op, executor, path, options) {
        Ok(value) => OpendalMetadataResult::ok(value as *mut c_void),
        Err(error) => OpendalMetadataResult::from_error(error),
    }
}

fn operator_stat_with_options_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::StatOptions,
) -> Result<*mut OpendalMetadata, OpenDALError> {
    let op = require_operator(op)?;
    let executor = unsafe { executor_or_default(executor) }?;
    let path = require_cstr(path, "path")?;
    let options = if options.is_null() {
        opendal::options::StatOptions::default()
    } else {
        unsafe { (&*options).clone() }
    };

    let metadata = executor
        .block_on(op.stat_options(path, options))
        .map_err(OpenDALError::from_opendal_error)?;
    Ok(Box::into_raw(Box::new(OpendalMetadata::from_metadata(metadata))))
}

/// List entries from `path` synchronously with options.
///
/// On success, returned payload must be released with `opendal_entry_list_result_release`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `option_len > 0`, `option_keys` and `option_values` must be valid arrays.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_list_with_options(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::ListOptions,
) -> OpendalEntryListResult {
    match operator_list_with_options_inner(op, executor, path, options) {
        Ok(value) => OpendalEntryListResult::ok(value),
        Err(error) => OpendalEntryListResult::from_error(error),
    }
}

fn operator_list_with_options_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::ListOptions,
) -> Result<*mut c_void, OpenDALError> {
    let op = require_operator(op)?;
    let executor = unsafe { executor_or_default(executor) }?;
    let path = require_cstr(path, "path")?;
    let options = if options.is_null() {
        opendal::options::ListOptions::default()
    } else {
        unsafe { (&*options).clone() }
    };

    let entries = executor
        .block_on(op.list_options(path, options))
        .map_err(OpenDALError::from_opendal_error)?;

    Ok(into_entry_list_ptr(entries))
}

/// Stat `path` asynchronously with options.
///
/// The callback is invoked exactly once. On success, callback result must be
/// released with `opendal_metadata_result_release`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `option_len > 0`, `option_keys` and `option_values` must be valid arrays.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_stat_with_options_async(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::StatOptions,
    callback: Option<StatCallback>,
    context: i64,
) -> OpendalResult {
    match operator_stat_with_options_async_inner(op, executor, path, options, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_stat_with_options_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::StatOptions,
    callback: Option<StatCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = unsafe { executor_or_default(executor) }?;
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let options = if options.is_null() {
        opendal::options::StatOptions::default()
    } else {
        unsafe { (&*options).clone() }
    };

    let op = op.clone();
    executor.spawn(async move {
        let result = op
            .stat_options(&path, options)
            .await
            .map(OpendalMetadata::from_metadata)
            .map(|v| Box::into_raw(Box::new(v)))
            .map_err(OpenDALError::from_opendal_error);

        unsafe {
            callback(
                context,
                match result {
                    Ok(value) => OpendalMetadataResult::ok(value as *mut c_void),
                    Err(error) => OpendalMetadataResult::from_error(error),
                },
            );
        }
    });

    Ok(())
}

/// List entries from `path` asynchronously with options.
///
/// The callback is invoked exactly once. On success, callback result must be
/// released with `opendal_entry_list_result_release`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `option_len > 0`, `option_keys` and `option_values` must be valid arrays.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_list_with_options_async(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::ListOptions,
    callback: Option<ListCallback>,
    context: i64,
) -> OpendalResult {
    match operator_list_with_options_async_inner(op, executor, path, options, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_list_with_options_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::ListOptions,
    callback: Option<ListCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = unsafe { executor_or_default(executor) }?;
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let options = if options.is_null() {
        opendal::options::ListOptions::default()
    } else {
        unsafe { (&*options).clone() }
    };

    let op = op.clone();
    executor.spawn(async move {
        let result = op
            .list_options(&path, options)
            .await
            .map(into_entry_list_ptr)
            .map_err(OpenDALError::from_opendal_error);

        unsafe {
            callback(
                context,
                match result {
                    Ok(value) => OpendalEntryListResult::ok(value),
                    Err(error) => OpendalEntryListResult::from_error(error),
                },
            );
        }
    });

    Ok(())
}
