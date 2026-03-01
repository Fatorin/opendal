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

use crate::error::OpenDALError;
use crate::utils::config_invalid_error;

/// Validate retry layer options.
pub fn validate_retry_options(
    factor: f32,
    min_delay_nanos: u64,
    max_delay_nanos: u64,
) -> Result<(), OpenDALError> {
    if !factor.is_finite() || factor <= 0.0 {
        return Err(config_invalid_error(
            "retry factor must be a positive finite number",
        ));
    }

    if max_delay_nanos < min_delay_nanos {
        return Err(config_invalid_error(
            "max_delay_nanos must be greater than or equal to min_delay_nanos",
        ));
    }

    Ok(())
}

/// Validate timeout layer options.
pub fn validate_timeout_options(timeout_nanos: u64, io_timeout_nanos: u64) -> Result<(), OpenDALError> {
    if timeout_nanos == 0 {
        return Err(config_invalid_error("timeout_nanos must be greater than zero"));
    }

    if io_timeout_nanos == 0 {
        return Err(config_invalid_error("io_timeout_nanos must be greater than zero"));
    }

    Ok(())
}

/// Validate concurrent-limit layer options.
pub fn validate_concurrent_limit_options(permits: usize) -> Result<(), OpenDALError> {
    if permits == 0 {
        return Err(config_invalid_error("permits must be greater than zero"));
    }

    Ok(())
}
