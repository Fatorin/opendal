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

//! Rust FFI layer backing the OpenDAL .NET binding.
//!
//! This crate exposes `extern "C"` APIs consumed by C# via P/Invoke and keeps
//! interop memory ownership explicit through dedicated release functions.

/// Error interop types and conversion helpers.
mod error;
/// Result wrappers and release entry points for FFI calls.
mod result;
/// Capability interop structures.
mod capability;
/// Operator-info interop structures.
mod operator_info;
/// Shared FFI utilities and pointer validation helpers.
mod utils;
/// Operator-related FFI APIs.
mod operator;
/// Byte buffer interop helpers.
mod byte_buffer;
/// Executor registry and runtime management for async operations.
mod executor;
/// Option parsing and typed option structures.
mod options;
/// Metadata interop structures.
mod metadata;
/// Entry/list interop structures.
mod entry;
/// Input validators for layer/options FFI.
mod validators;