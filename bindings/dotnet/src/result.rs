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

#[repr(C)]
/// Result for operations that only report success or failure.
pub struct OpendalResult {
    /// Error information for the operation.
    pub error: OpenDALError,
}

#[repr(C)]
/// Result for operations returning a native pointer.
pub struct OpendalPointerResult {
    /// Pointer payload on success, null on error.
    pub ptr: *mut c_void,
    /// Error information for the operation.
    pub error: OpenDALError,
}

#[repr(C)]
/// Result for operations returning a byte buffer.
pub struct OpendalByteBufferResult {
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
    OpendalPointerResult,
    field = ptr: *mut c_void,
    error_value = std::ptr::null_mut()
);

define_result!(
    OpendalByteBufferResult,
    field = buffer: ByteBuffer,
    error_value = ByteBuffer::empty()
);
