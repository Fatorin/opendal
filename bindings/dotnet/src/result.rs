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

use std::ffi::c_void;

use crate::byte_buffer::ByteBuffer;
use crate::error::OpenDALError;
use crate::entry::entry_list_free;
use crate::metadata::metadata_free;
use crate::operator::operator_info_free;
use crate::utils::buffer_free;

#[repr(C)]
/// Result for operations that only report success or failure.
pub struct OpendalResult {
    /// Error information for the operation.
    pub error: OpenDALError,
}

#[repr(C)]
/// Result for operations returning an operator handle pointer.
pub struct OpendalOperatorResult {
    /// Pointer payload on success, null on error.
    pub ptr: *mut c_void,
    /// Error information for the operation.
    pub error: OpenDALError,
}

#[repr(C)]
/// Result for operations returning an options handle pointer.
pub struct OpendalOptionsResult {
    /// Pointer payload on success, null on error.
    pub ptr: *mut c_void,
    /// Error information for the operation.
    pub error: OpenDALError,
}

#[repr(C)]
/// Result for operations returning an executor handle pointer.
pub struct OpendalExecutorResult {
    /// Pointer payload on success, null on error.
    pub ptr: *mut c_void,
    /// Error information for the operation.
    pub error: OpenDALError,
}

#[repr(C)]
/// Result for operations returning an operator info payload pointer.
pub struct OpendalOperatorInfoResult {
    /// Pointer payload on success, null on error.
    pub ptr: *mut c_void,
    /// Error information for the operation.
    pub error: OpenDALError,
}

#[repr(C)]
/// Result for operations returning a metadata payload pointer.
pub struct OpendalMetadataResult {
    /// Pointer payload on success, null on error.
    pub ptr: *mut c_void,
    /// Error information for the operation.
    pub error: OpenDALError,
}

#[repr(C)]
/// Result for operations returning an entry list payload pointer.
pub struct OpendalEntryListResult {
    /// Pointer payload on success, null on error.
    pub ptr: *mut c_void,
    /// Error information for the operation.
    pub error: OpenDALError,
}

#[repr(C)]
/// Result for operations returning a byte buffer payload.
pub struct OpendalReadResult {
    /// Buffer payload on success.
    pub buffer: ByteBuffer,
    /// Error information for the operation.
    pub error: OpenDALError,
}

macro_rules! define_result {
    ($result_ty:ident) => {
        impl $result_ty {
            pub fn ok() -> Self {
                Self {
                    error: OpenDALError::ok(),
                }
            }

            pub fn from_error(error: OpenDALError) -> Self {
                Self { error }
            }
        }
    };

    (
        $result_ty:ident,
        field = $field:ident : $payload_ty:ty,
        error_value = $error_value:expr
    ) => {
        impl $result_ty {
            pub fn ok($field: $payload_ty) -> Self {
                Self {
                    $field,
                    error: OpenDALError::ok(),
                }
            }

            pub fn from_error(error: OpenDALError) -> Self {
                Self {
                    $field: $error_value,
                    error,
                }
            }
        }
    };
}

define_result!(OpendalResult);

define_result!(
    OpendalOperatorResult,
    field = ptr: *mut c_void,
    error_value = std::ptr::null_mut()
);

define_result!(
    OpendalOptionsResult,
    field = ptr: *mut c_void,
    error_value = std::ptr::null_mut()
);

define_result!(
    OpendalExecutorResult,
    field = ptr: *mut c_void,
    error_value = std::ptr::null_mut()
);

define_result!(
    OpendalOperatorInfoResult,
    field = ptr: *mut c_void,
    error_value = std::ptr::null_mut()
);

define_result!(
    OpendalMetadataResult,
    field = ptr: *mut c_void,
    error_value = std::ptr::null_mut()
);

define_result!(
    OpendalEntryListResult,
    field = ptr: *mut c_void,
    error_value = std::ptr::null_mut()
);

define_result!(
    OpendalReadResult,
    field = buffer: ByteBuffer,
    error_value = ByteBuffer::empty()
);

fn release_error_message(error: &mut OpenDALError) {
    if error.message.is_null() {
        return;
    }

    unsafe {
        drop(std::ffi::CString::from_raw(error.message));
    }
    error.message = std::ptr::null_mut();
}

#[unsafe(no_mangle)]
pub extern "C" fn opendal_result_release(mut result: OpendalResult) {
    release_error_message(&mut result.error);
}

#[unsafe(no_mangle)]
pub extern "C" fn opendal_operator_result_release(mut result: OpendalOperatorResult) {
    release_error_message(&mut result.error);
}

#[unsafe(no_mangle)]
pub extern "C" fn opendal_options_result_release(mut result: OpendalOptionsResult) {
    release_error_message(&mut result.error);
}

#[unsafe(no_mangle)]
pub extern "C" fn opendal_executor_result_release(mut result: OpendalExecutorResult) {
    release_error_message(&mut result.error);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_operator_info_result_release(mut result: OpendalOperatorInfoResult) {
    if !result.ptr.is_null() {
        unsafe {
            operator_info_free(result.ptr.cast());
        }
    }

    release_error_message(&mut result.error);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_metadata_result_release(mut result: OpendalMetadataResult) {
    if !result.ptr.is_null() {
        unsafe {
            metadata_free(result.ptr.cast());
        }
    }

    release_error_message(&mut result.error);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_entry_list_result_release(mut result: OpendalEntryListResult) {
    if !result.ptr.is_null() {
        unsafe {
            entry_list_free(result.ptr.cast());
        }
    }

    release_error_message(&mut result.error);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_read_result_release(mut result: OpendalReadResult) {
    if !result.buffer.data.is_null() {
        unsafe {
            buffer_free(result.buffer.data, result.buffer.len, result.buffer.capacity);
        }
    }

    release_error_message(&mut result.error);
}
