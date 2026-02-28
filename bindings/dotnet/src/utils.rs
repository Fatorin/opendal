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

use std::collections::HashMap;
use std::os::raw::c_char;

use crate::error::{ErrorCode, OpenDALError};

/// Convert a C string pointer into `&str`.
///
/// Returns `None` when the pointer is null or not valid UTF-8.
pub fn cstr_to_str<'a>(value: *const c_char) -> Option<&'a str> {
    if value.is_null() {
        return None;
    }

    let cstr = unsafe { std::ffi::CStr::from_ptr(value) };
    cstr.to_str().ok()
}

/// Build a `ConfigInvalid` error payload for invalid FFI inputs.
pub fn config_invalid_error(message: impl Into<String>) -> OpenDALError {
    OpenDALError::from_error(ErrorCode::ConfigInvalid, message.into())
}

/// Build the standard invalid UTF-8 message for a field.
pub fn invalid_utf8_message(field: &str) -> String {
    format!("{field} is null or invalid UTF-8")
}

/// Build the standard invalid UTF-8 message for an indexed field.
pub fn invalid_utf8_message_at(field: &str, index: usize) -> String {
    format!("{field} at index {index} is null or invalid UTF-8")
}

/// Require a non-null, UTF-8 C string pointer.
pub fn require_cstr<'a>(value: *const c_char, field: &str) -> Result<&'a str, OpenDALError> {
    cstr_to_str(value)
        .ok_or_else(|| OpenDALError::from_error(ErrorCode::ConfigInvalid, invalid_utf8_message(field)))
}

/// Require a non-null, UTF-8 C string pointer for an indexed argument.
pub fn require_cstr_at<'a>(
    value: *const c_char,
    field: &str,
    index: usize,
) -> Result<&'a str, OpenDALError> {
    cstr_to_str(value).ok_or_else(|| config_invalid_error(invalid_utf8_message_at(field, index)))
}

/// Require a non-null operator pointer.
pub fn require_operator<'a>(
    op: *const opendal::Operator,
) -> Result<&'a opendal::Operator, OpenDALError> {
    if op.is_null() {
        return Err(config_invalid_error("operator pointer is null"));
    }

    Ok(unsafe { &*op })
}

/// Require a non-null callback function pointer.
pub fn require_callback<T>(callback: Option<T>) -> Result<T, OpenDALError> {
    callback.ok_or_else(|| config_invalid_error("callback pointer is null"))
}

/// Require a non-null data pointer when `len > 0`.
pub fn require_data_ptr(data: *const u8, len: usize) -> Result<(), OpenDALError> {
    if len > 0 && data.is_null() {
        return Err(config_invalid_error("data pointer is null while len > 0"));
    }

    Ok(())
}

/// Collect option key/value C-string arrays into a Rust map.
///
/// Returns `ConfigInvalid` when array pointers are invalid or any entry is not
/// valid UTF-8.
///
/// # Safety
///
/// - When `len > 0`, `keys` and `values` must be non-null and point to arrays
///   with at least `len` entries.
/// - Every key/value entry must point to a valid null-terminated UTF-8 string.
pub unsafe fn collect_options(
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
) -> Result<HashMap<String, String>, OpenDALError> {
    if len > 0 && (keys.is_null() || values.is_null()) {
        return Err(config_invalid_error(
            "keys or values pointer is null while len > 0",
        ));
    }

    let mut options = HashMap::<String, String>::with_capacity(len);
    for index in 0..len {
        let key_ptr = unsafe { *keys.add(index) };
        let value_ptr = unsafe { *values.add(index) };

        let key = require_cstr_at(key_ptr, "key", index)?;
        let value = require_cstr_at(value_ptr, "value", index)?;
        options.insert(key.to_string(), value.to_string());
    }

    Ok(options)
}

/// # Safety
///
/// - `data`, `len`, and `capacity` must come from `ByteBuffer::from_vec`.
/// - `buffer_free` must be called at most once for the same allocation.
/// - Callers must not access `data` after this function returns.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn buffer_free(data: *mut u8, len: usize, capacity: usize) {
    if data.is_null() {
        debug_assert_eq!(len, 0, "len must be zero when data is null");
        debug_assert_eq!(capacity, 0, "capacity must be zero when data is null");
        return;
    }

    if capacity == 0 {
        debug_assert!(
            capacity > 0,
            "capacity must be greater than zero when data is not null"
        );
        return;
    }

    if capacity < len {
        debug_assert!(
            capacity >= len,
            "capacity must be greater than or equal to len"
        );
        return;
    }

    unsafe {
        drop(Vec::from_raw_parts(data, len, capacity));
    }
}

pub fn into_string_ptr(message: impl Into<String>) -> *mut c_char {
    match std::ffi::CString::new(message.into()) {
        Ok(msg) => msg.into_raw(),
        Err(_) => std::ffi::CString::new("invalid error message")
            .unwrap()
            .into_raw(),
    }
}

/// # Safety
///
/// - `message` must be a pointer returned by Rust via `CString::into_raw`.
/// - `message_free` must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn string_ptr_free(message: *mut c_char) {
    if message.is_null() {
        return;
    }

    unsafe {
        drop(std::ffi::CString::from_raw(message));
    }
}
