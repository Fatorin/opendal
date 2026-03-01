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
    executor::executor_or_default,
    error::OpenDALError,
    metadata::OpendalMetadata,
    options::{parse_read_options, parse_stat_options, parse_write_options},
    operator_info::OpendalOperatorInfo,
    result::{
        OpendalByteBufferResult, OpendalIntPtrResult, OpendalMetadataResult, OpendalResult,
    },
    utils::{
        collect_options, into_operator_info, require_callback, require_cstr, require_data_ptr,
        require_operator,
    },
};

use std::ffi::c_void;
use std::os::raw::c_char;

type WriteCallback = unsafe extern "C" fn(context: *mut c_void, result: OpendalResult);
type ReadCallback = unsafe extern "C" fn(context: *mut c_void, result: OpendalByteBufferResult);
type StatCallback = unsafe extern "C" fn(context: *mut c_void, result: OpendalMetadataResult);

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
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
) -> OpendalIntPtrResult {
    match unsafe { operator_construct_inner(scheme, keys, values, len) } {
        Ok(op) => OpendalIntPtrResult::ok(op),
        Err(error) => OpendalIntPtrResult::from_error(error),
    }
}

unsafe fn operator_construct_inner(
    scheme: *const c_char,
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
) -> Result<*mut c_void, OpenDALError> {
    let scheme = require_cstr(scheme, "scheme")?;
    let options = unsafe { collect_options(keys, values, len) }?;
    let op = opendal::Operator::via_iter(scheme, options).map_err(OpenDALError::from_opendal_error)?;
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
/// On success, returned string fields in payload must be released by
/// `string_ptr_free`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_info_get(
    op: *const opendal::Operator,
) -> OpendalIntPtrResult {
    match unsafe { operator_info_get_inner(op) } {
        Ok(value) => OpendalIntPtrResult::ok(value),
        Err(error) => OpendalIntPtrResult::from_error(error),
    }
}

unsafe fn operator_info_get_inner(
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

/// Write bytes to `path` synchronously.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `len > 0`, `data` must be non-null and readable for `len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_write(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    data: *const u8,
    len: usize,
) -> OpendalResult {
    match unsafe { operator_write_inner(op, executor, path, data, len) } {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

unsafe fn operator_write_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    data: *const u8,
    len: usize,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = unsafe { executor_or_default(executor) }?;
    let path = require_cstr(path, "path")?;
    require_data_ptr(data, len)?;

    let payload = if len == 0 {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(data, len) }
    };

    executor
        .block_on(op.write_options(path, payload, opendal::options::WriteOptions::default()))
        .map(|_| ())
        .map_err(OpenDALError::from_opendal_error)
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
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    option_len: usize,
) -> OpendalResult {
    match unsafe {
        operator_write_with_options_inner(
            op,
            executor,
            path,
            data,
            len,
            option_keys,
            option_values,
            option_len,
        )
    } {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

unsafe fn operator_write_with_options_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    data: *const u8,
    len: usize,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    option_len: usize,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = unsafe { executor_or_default(executor) }?;
    let path = require_cstr(path, "path")?;
    require_data_ptr(data, len)?;
    let values = unsafe { collect_options(option_keys, option_values, option_len) }?;
    let options = parse_write_options(&values)?;

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

/// Read all bytes from `path` synchronously.
///
/// On success, the returned buffer must be released with `buffer_free`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_read(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
) -> OpendalByteBufferResult {
    match unsafe { operator_read_inner(op, executor, path) } {
        Ok(value) => OpendalByteBufferResult::ok(value),
        Err(error) => OpendalByteBufferResult::from_error(error),
    }
}

unsafe fn operator_read_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
) -> Result<ByteBuffer, OpenDALError> {
    let op = require_operator(op)?;
    let executor = unsafe { executor_or_default(executor) }?;
    let path = require_cstr(path, "path")?;
    let value = executor
        .block_on(op.read(path))
        .map(|v| v.to_vec())
        .map_err(OpenDALError::from_opendal_error)?;

    Ok(ByteBuffer::from_vec(value))
}

/// Read bytes from `path` synchronously with options.
///
/// On success, the returned buffer must be released with `buffer_free`.
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
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    option_len: usize,
) -> OpendalByteBufferResult {
    match unsafe {
        operator_read_with_options_inner(op, executor, path, option_keys, option_values, option_len)
    } {
        Ok(value) => OpendalByteBufferResult::ok(value),
        Err(error) => OpendalByteBufferResult::from_error(error),
    }
}

unsafe fn operator_read_with_options_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    option_len: usize,
) -> Result<ByteBuffer, OpenDALError> {
    let op = require_operator(op)?;
    let executor = unsafe { executor_or_default(executor) }?;
    let path = require_cstr(path, "path")?;
    let values = unsafe { collect_options(option_keys, option_values, option_len) }?;
    let options = parse_read_options(&values)?;

    let value = executor
        .block_on(op.read_options(path, options))
        .map(|v| v.to_vec())
        .map_err(OpenDALError::from_opendal_error)?;

    Ok(ByteBuffer::from_vec(value))
}

/// Stat `path` synchronously with options.
///
/// On success, returned payload must be released with `metadata_free`.
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
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    option_len: usize,
) -> OpendalMetadataResult {
    match unsafe {
        operator_stat_with_options_inner(op, executor, path, option_keys, option_values, option_len)
    } {
        Ok(value) => OpendalMetadataResult::ok(value),
        Err(error) => OpendalMetadataResult::from_error(error),
    }
}

unsafe fn operator_stat_with_options_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    option_len: usize,
) -> Result<*mut OpendalMetadata, OpenDALError> {
    let op = require_operator(op)?;
    let executor = unsafe { executor_or_default(executor) }?;
    let path = require_cstr(path, "path")?;
    let values = unsafe { collect_options(option_keys, option_values, option_len) }?;
    let options = parse_stat_options(&values)?;

    let metadata = executor
        .block_on(op.stat_options(path, options))
        .map_err(OpenDALError::from_opendal_error)?;
    Ok(Box::into_raw(Box::new(OpendalMetadata::from_metadata(metadata))))
}

/// Write bytes to `path` asynchronously.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `len > 0`, `data` must be non-null and readable for `len` bytes.
/// - `callback` must be a valid function pointer and remain callable until it
///   is invoked.
/// - `context` is passed through as-is to `callback` and must remain valid for
///   the callback's usage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_write_async(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    data: *const u8,
    len: usize,
    callback: Option<WriteCallback>,
    context: *mut c_void,
) -> OpendalResult {
    match unsafe { operator_write_async_inner(op, executor, path, data, len, callback, context) } {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

unsafe fn operator_write_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    data: *const u8,
    len: usize,
    callback: Option<WriteCallback>,
    context: *mut c_void,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = unsafe { executor_or_default(executor) }?;
    let path = require_cstr(path, "path")?.to_string();
    require_data_ptr(data, len)?;
    let callback = require_callback(callback)?;

    let payload = if len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(data, len) }.to_vec()
    };

    let op = op.clone();
    let context = context as usize;
    executor.spawn(async move {
        let result = op
            .write(&path, payload)
            .await
            .map(|_| ())
            .map_err(OpenDALError::from_opendal_error);

        unsafe {
            callback(
                context as *mut c_void,
                match result {
                    Ok(()) => OpendalResult::ok(),
                    Err(error) => OpendalResult::from_error(error),
                },
            );
        }
    });

    Ok(())
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
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    option_len: usize,
    callback: Option<WriteCallback>,
    context: *mut c_void,
) -> OpendalResult {
    match unsafe {
        operator_write_with_options_async_inner(
            op,
            executor,
            path,
            data,
            len,
            option_keys,
            option_values,
            option_len,
            callback,
            context,
        )
    } {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

unsafe fn operator_write_with_options_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    data: *const u8,
    len: usize,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    option_len: usize,
    callback: Option<WriteCallback>,
    context: *mut c_void,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = unsafe { executor_or_default(executor) }?;
    let path = require_cstr(path, "path")?.to_string();
    require_data_ptr(data, len)?;
    let callback = require_callback(callback)?;
    let values = unsafe { collect_options(option_keys, option_values, option_len) }?;
    let options = parse_write_options(&values)?;

    let payload = if len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(data, len) }.to_vec()
    };

    let op = op.clone();
    let context = context as usize;
    executor.spawn(async move {
        let result = op
            .write_options(&path, payload, options)
            .await
            .map(|_| ())
            .map_err(OpenDALError::from_opendal_error);

        unsafe {
            callback(
                context as *mut c_void,
                match result {
                    Ok(()) => OpendalResult::ok(),
                    Err(error) => OpendalResult::from_error(error),
                },
            );
        }
    });

    Ok(())
}

/// Read bytes from `path` asynchronously.
///
/// The callback is invoked exactly once. On successful reads, the returned
/// buffer in callback result must be released with `buffer_free`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `callback` must be a valid function pointer and remain callable until it
///   is invoked.
/// - `context` is passed through as-is to `callback` and must remain valid for
///   the callback's usage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_read_async(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    callback: Option<ReadCallback>,
    context: *mut c_void,
) -> OpendalResult {
    match unsafe { operator_read_async_inner(op, executor, path, callback, context) } {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

unsafe fn operator_read_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    callback: Option<ReadCallback>,
    context: *mut c_void,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = unsafe { executor_or_default(executor) }?;
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;

    let op = op.clone();
    let context = context as usize;
    executor.spawn(async move {
        let result = op
            .read(&path)
            .await
            .map(|v| ByteBuffer::from_vec(v.to_vec()))
            .map_err(OpenDALError::from_opendal_error);

        unsafe {
            callback(
                context as *mut c_void,
                match result {
                    Ok(value) => OpendalByteBufferResult::ok(value),
                    Err(error) => OpendalByteBufferResult::from_error(error),
                },
            );
        }
    });

    Ok(())
}

/// Read bytes from `path` asynchronously with options.
///
/// The callback is invoked exactly once. On successful reads, the returned
/// buffer in callback result must be released with `buffer_free`.
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
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    option_len: usize,
    callback: Option<ReadCallback>,
    context: *mut c_void,
) -> OpendalResult {
    match unsafe {
        operator_read_with_options_async_inner(
            op,
            executor,
            path,
            option_keys,
            option_values,
            option_len,
            callback,
            context,
        )
    } {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

unsafe fn operator_read_with_options_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    option_len: usize,
    callback: Option<ReadCallback>,
    context: *mut c_void,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = unsafe { executor_or_default(executor) }?;
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let values = unsafe { collect_options(option_keys, option_values, option_len) }?;
    let options = parse_read_options(&values)?;

    let op = op.clone();
    let context = context as usize;
    executor.spawn(async move {
        let result = op
            .read_options(&path, options)
            .await
            .map(|v| ByteBuffer::from_vec(v.to_vec()))
            .map_err(OpenDALError::from_opendal_error);

        unsafe {
            callback(
                context as *mut c_void,
                match result {
                    Ok(value) => OpendalByteBufferResult::ok(value),
                    Err(error) => OpendalByteBufferResult::from_error(error),
                },
            );
        }
    });

    Ok(())
}

/// Stat `path` asynchronously with options.
///
/// The callback is invoked exactly once. On success, returned metadata payload
/// in callback result must be released with `metadata_free`.
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
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    option_len: usize,
    callback: Option<StatCallback>,
    context: *mut c_void,
) -> OpendalResult {
    match unsafe {
        operator_stat_with_options_async_inner(
            op,
            executor,
            path,
            option_keys,
            option_values,
            option_len,
            callback,
            context,
        )
    } {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

unsafe fn operator_stat_with_options_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    option_len: usize,
    callback: Option<StatCallback>,
    context: *mut c_void,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = unsafe { executor_or_default(executor) }?;
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let values = unsafe { collect_options(option_keys, option_values, option_len) }?;
    let options = parse_stat_options(&values)?;

    let op = op.clone();
    let context = context as usize;
    executor.spawn(async move {
        let result = op
            .stat_options(&path, options)
            .await
            .map(OpendalMetadata::from_metadata)
            .map(|v| Box::into_raw(Box::new(v)))
            .map_err(OpenDALError::from_opendal_error);

        unsafe {
            callback(
                context as *mut c_void,
                match result {
                    Ok(value) => OpendalMetadataResult::ok(value),
                    Err(error) => OpendalMetadataResult::from_error(error),
                },
            );
        }
    });

    Ok(())
}
