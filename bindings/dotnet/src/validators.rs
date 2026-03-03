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

pub mod layer;
pub mod options;

pub(crate) mod prelude {
    pub(crate) use super::layer::{
        validate_concurrent_limit_options, validate_retry_options, validate_timeout_options,
    };
    pub(crate) use super::options::{
        validate_list_limit, validate_read_chunk, validate_read_concurrent, validate_read_gap,
        validate_read_range_end, validate_write_chunk, validate_write_concurrent,
    };
}
