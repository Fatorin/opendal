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

//! Options module provides options definitions for operations.
//!
//! # Deserialization
//!
//! Every options struct implements [`serde::Deserialize`], so a binding can
//! pass options across its FFI boundary as one serialized value, such as a
//! JSON object, instead of mirroring each field. The representation is:
//!
//! - Members use the field names. A missing member keeps the struct's
//!   [`Default`] value, and an unknown member is an error, so a misspelled
//!   option fails instead of being ignored.
//! - Timestamps such as `if_modified_since` are Unix milliseconds.
//! - `range` is `{"offset": .., "size": ..}`, where both members are optional
//!   and a missing `size` reads to the end, or `{"suffix": ..}` to read the
//!   last bytes. `suffix` cannot be combined with `offset` or `size`.
//! - `if_not_changed` is `{"etag": .., "version": ..}`, the two values the
//!   condition is derived from. Both members are optional; an object with
//!   neither still deserializes, so the operation rejects it instead of
//!   silently dropping the condition.
//!
//! ```
//! # use opendal_core::options::ReadOptions;
//! let options: ReadOptions = serde_json::from_str(
//!     r#"{"range": {"suffix": 1024}, "if_modified_since": 1700000000000}"#,
//! )
//! .unwrap();
//! assert!(options.range.is_suffix());
//! ```

use crate::Metadata;
use crate::raw::Timestamp;
use crate::types::BytesRange;
use serde::Deserialize;
use std::collections::HashMap;

/// Options for delete operations.
///
/// Each condition checks the file currently stored at the delete path, and
/// every condition must hold for the delete to proceed:
///
/// - A false condition fails the delete with
///   [`crate::ErrorKind::ConditionNotMatch`].
/// - A condition the service does not advertise through its capability fails
///   the delete with [`crate::ErrorKind::Unsupported`]; OpenDAL never
///   silently drops a condition. A service that cannot preserve a
///   combination of conditions also returns `Unsupported`.
/// - A service-side conflict unrelated to these conditions surfaces as
///   [`crate::ErrorKind::Conflict`].
///
/// Once every condition holds, deleting a missing file remains a successful
/// no-op.
///
/// See the [conditional operation specification][crate::docs::specs::conditional_operations]
/// for the complete cross-operation contract.
#[derive(Debug, Clone, Default, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DeleteOptions {
    /// Delete the given version of the file instead of the current one.
    ///
    /// This selects which stored version the delete removes; it is not a
    /// condition on the file.
    ///
    /// Check [`crate::Capability::delete_with_version`] before using this
    /// option.
    pub version: Option<String>,
    /// Whether to delete the target recursively.
    ///
    /// - If `false`, behaves like the traditional single-object delete.
    /// - If `true`, all entries under the path (or sharing the prefix for file-like paths)
    ///   will be removed.
    pub recursive: bool,
    /// Delete only when the file at the delete path has this exact ETag.
    ///
    /// The condition succeeds when the file exists and its ETag equals this
    /// value. A file with a different ETag, or a missing file, fails the
    /// delete with [`crate::ErrorKind::ConditionNotMatch`]. Only concrete
    /// ETag values are portable; a wildcard such as `"*"` has no portable
    /// meaning here.
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise [`crate::Capability::delete_with_if_match`].
    pub if_match: Option<String>,
    /// Delete only when the file at the delete path does not have this ETag.
    ///
    /// With a concrete ETag value, the condition succeeds when the file has
    /// a different ETag or does not exist; deleting a missing file is then a
    /// successful no-op. A file whose ETag equals this value fails the
    /// delete with [`crate::ErrorKind::ConditionNotMatch`]. Only concrete
    /// ETag values are portable; a wildcard such as `"*"` has no portable
    /// meaning here.
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::delete_with_if_none_match`].
    pub if_none_match: Option<String>,
    /// Delete only when the file at the delete path has this exact version.
    ///
    /// The condition succeeds when the file exists and its version equals
    /// this value. A file with a different version, or a missing file, fails
    /// the delete with [`crate::ErrorKind::ConditionNotMatch`].
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::delete_with_if_version_match`].
    pub if_version_match: Option<String>,
    /// Delete only when the file at the delete path does not have this
    /// version.
    ///
    /// The condition succeeds when the file exists with a different version.
    /// A file whose version equals this value fails the delete with
    /// [`crate::ErrorKind::ConditionNotMatch`]. When no file exists at the
    /// delete path, no portable behavior is defined.
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::delete_with_if_version_not_match`].
    pub if_version_not_match: Option<String>,
    /// Delete only when the file at the delete path still has the identity
    /// recorded in this metadata.
    ///
    /// Pass metadata previously returned by OpenDAL for the same path.
    /// OpenDAL derives a version match when the service supports version
    /// conditions and the metadata contains a version. Otherwise it derives
    /// an ETag match when possible. A changed or missing file fails the delete
    /// with [`crate::ErrorKind::ConditionNotMatch`], as does combining this
    /// option with a conflicting `if_match` or `if_version_match` value.
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not support the derived primitive condition, and
    /// [`crate::ErrorKind::ConfigInvalid`] when the metadata contains neither
    /// a version nor an ETag.
    #[serde(deserialize_with = "de::option_metadata_identity")]
    pub if_not_changed: Option<Metadata>,
}

/// Options for list operations.
///
/// # Groups
/// - Traversal: `recursive`.
/// - Pagination: `limit`, `start_after`.
/// - Versioning: `versions`, `deleted` (effective on version-aware backends).

#[derive(Debug, Clone, Default, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ListOptions {
    /// Maximum results per request (backend hint) to control memory and throttling.
    pub limit: Option<usize>,
    /// The start_after passes to underlying service to specify the specified key
    /// to start listing from.
    pub start_after: Option<String>,
    /// Whether to list recursively under the prefix; default `false`.
    pub recursive: bool,
    /// Include object versions when supported by the backend; default `false`.
    pub versions: bool,
    /// Include delete markers when supported by version-aware backends; default `false`.
    pub deleted: bool,
}

/// Options for read operations.
///
/// Each condition checks the file being read, and every condition must hold
/// for the read to proceed. A missing file fails the read with
/// [`crate::ErrorKind::NotFound`] no matter which conditions are set. An
/// existing file that fails a condition returns
/// [`crate::ErrorKind::ConditionNotMatch`], and a condition the service does
/// not advertise through its capability returns
/// [`crate::ErrorKind::Unsupported`]; OpenDAL never silently drops a
/// condition.
///
/// See the [conditional operation specification][crate::docs::specs::conditional_operations]
/// for the complete cross-operation contract.
#[derive(Debug, Clone, Default, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReadOptions {
    /// Set `range` for this operation.
    ///
    /// If we have a file with size `n`.
    ///
    /// - `..` means read bytes in range `[0, n)` of file.
    /// - `0..1024` and `..1024` means read bytes in range `[0, 1024)` of file
    /// - `1024..` means read bytes in range `[1024, n)` of file
    /// - `BytesRange::suffix(1024)` means read the last `min(1024, n)` bytes of file
    ///
    /// The type implements `From<RangeBounds<u64>>`, so users can use `(1024..).into()` instead.
    #[serde(deserialize_with = "de::bytes_range")]
    pub range: BytesRange,
    /// Read the given version of the file instead of the current one.
    ///
    /// This selects which stored version the read returns; it is not a
    /// condition on the file. A version that does not exist returns
    /// [`crate::ErrorKind::NotFound`].
    ///
    /// Check [`crate::Capability::read_with_version`] before using this
    /// option.
    pub version: Option<String>,

    /// Read only when the file has this exact ETag.
    ///
    /// The condition succeeds when the file exists and its ETag equals this
    /// value. A file with a different ETag returns
    /// [`crate::ErrorKind::ConditionNotMatch`]; a missing file returns
    /// [`crate::ErrorKind::NotFound`]. Only concrete ETag values are
    /// portable; a wildcard such as `"*"` has no portable meaning here.
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise [`crate::Capability::read_with_if_match`].
    pub if_match: Option<String>,
    /// Read only when the file does not have this ETag.
    ///
    /// With a concrete ETag value, the condition succeeds when the file
    /// exists with a different ETag. A file whose ETag equals this value
    /// returns [`crate::ErrorKind::ConditionNotMatch`]; a missing file
    /// returns [`crate::ErrorKind::NotFound`]. Only concrete ETag values
    /// are portable; a wildcard such as `"*"` has no portable meaning here.
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::read_with_if_none_match`].
    pub if_none_match: Option<String>,
    /// Read only when the file has this exact version.
    ///
    /// The condition succeeds when the file exists and its version equals
    /// this value. A file with a different version returns
    /// [`crate::ErrorKind::ConditionNotMatch`]; a missing file returns
    /// [`crate::ErrorKind::NotFound`].
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::read_with_if_version_match`].
    pub if_version_match: Option<String>,
    /// Read only when the file does not have this version.
    ///
    /// The condition succeeds when the file exists with a different version.
    /// A file whose version equals this value returns
    /// [`crate::ErrorKind::ConditionNotMatch`]; a missing file returns
    /// [`crate::ErrorKind::NotFound`].
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::read_with_if_version_not_match`].
    pub if_version_not_match: Option<String>,
    /// Read only when the file was modified after this timestamp.
    ///
    /// The condition succeeds when the file exists and was modified after
    /// this time. A file not modified since this time returns
    /// [`crate::ErrorKind::ConditionNotMatch`]; a missing file returns
    /// [`crate::ErrorKind::NotFound`].
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::read_with_if_modified_since`].
    #[serde(deserialize_with = "de::option_timestamp_millis")]
    pub if_modified_since: Option<Timestamp>,
    /// Read only when the file was not modified after this timestamp.
    ///
    /// The condition succeeds when the file exists and was not modified
    /// after this time. A file modified after this time returns
    /// [`crate::ErrorKind::ConditionNotMatch`]; a missing file returns
    /// [`crate::ErrorKind::NotFound`].
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::read_with_if_unmodified_since`].
    #[serde(deserialize_with = "de::option_timestamp_millis")]
    pub if_unmodified_since: Option<Timestamp>,

    /// Known content length of the object.
    ///
    /// This is an execution hint that allows OpenDAL to avoid extra metadata
    /// requests while planning reads. It must not be used as an object identity
    /// or consistency condition.
    pub content_length_hint: Option<u64>,

    /// Set `concurrent` for the operation.
    ///
    /// OpenDAL by default to read file without concurrent. This is not efficient for cases when users
    /// read large chunks of data. By setting `concurrent`, opendal will reading files concurrently
    /// on support storage services.
    ///
    /// By setting `concurrent`, opendal will fetch chunks concurrently with
    /// the give chunk size.
    ///
    /// Refer to the [performance guide](https://github.com/apache/opendal/tree/main/core/core/src/docs/performance)
    /// for more details.
    pub concurrent: usize,
    /// Set `chunk` for the operation.
    ///
    /// OpenDAL will use services' preferred chunk size by default. Users can set chunk based on their own needs.
    ///
    /// Refer to the [performance guide](https://github.com/apache/opendal/tree/main/core/core/src/docs/performance)
    /// for more details.
    pub chunk: Option<usize>,
    /// Controls the optimization strategy for range reads in [`crate::Reader::fetch`].
    ///
    /// When performing range reads, if the gap between two requested ranges is less than or
    /// equal to the configured `gap` size, OpenDAL will merge these ranges into a single read request
    /// and discard the unrequested data in between. This helps reduce the number of API calls
    /// to remote storage services.
    ///
    /// Set to `0` to disable merging ranges separated by a gap. Overlapping or adjacent ranges
    /// are still merged.
    ///
    /// This optimization is particularly useful when performing multiple small range reads
    /// that are close to each other, as it reduces the overhead of multiple network requests
    /// at the cost of transferring some additional data.
    ///
    /// Refer to the [performance guide](https://github.com/apache/opendal/tree/main/core/core/src/docs/performance)
    /// for more details.
    pub gap: Option<usize>,

    /// Specify the content-type header that should be sent back by the operation.
    ///
    /// This option is only meaningful when used along with presign.
    pub override_content_type: Option<String>,
    /// Specify the `cache-control` header that should be sent back by the operation.
    ///
    /// This option is only meaningful when used along with presign.
    pub override_cache_control: Option<String>,
    /// Specify the `content-disposition` header that should be sent back by the operation.
    ///
    /// This option is only meaningful when used along with presign.
    pub override_content_disposition: Option<String>,
}

/// Options for reader operations.
///
/// Each condition checks the file being read, and every condition must hold
/// for the reader to produce data. A missing file fails with
/// [`crate::ErrorKind::NotFound`] no matter which conditions are set. An
/// existing file that fails a condition returns
/// [`crate::ErrorKind::ConditionNotMatch`], and a condition the service does
/// not advertise through its capability returns
/// [`crate::ErrorKind::Unsupported`]; OpenDAL never silently drops a
/// condition. Depending on the service, a conditional error may surface when
/// creating the reader or while reading from it.
///
/// See the [conditional operation specification][crate::docs::specs::conditional_operations]
/// for the complete cross-operation contract.
#[derive(Debug, Clone, Default, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReaderOptions {
    /// Read the given version of the file instead of the current one.
    ///
    /// This selects which stored version the reader returns; it is not a
    /// condition on the file. A version that does not exist returns
    /// [`crate::ErrorKind::NotFound`].
    ///
    /// Check [`crate::Capability::read_with_version`] before using this
    /// option.
    pub version: Option<String>,

    /// Read only when the file has this exact ETag.
    ///
    /// The condition succeeds when the file exists and its ETag equals this
    /// value. A file with a different ETag returns
    /// [`crate::ErrorKind::ConditionNotMatch`]; a missing file returns
    /// [`crate::ErrorKind::NotFound`]. Only concrete ETag values are
    /// portable; a wildcard such as `"*"` has no portable meaning here.
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise [`crate::Capability::read_with_if_match`].
    pub if_match: Option<String>,
    /// Read only when the file does not have this ETag.
    ///
    /// With a concrete ETag value, the condition succeeds when the file
    /// exists with a different ETag. A file whose ETag equals this value
    /// returns [`crate::ErrorKind::ConditionNotMatch`]; a missing file
    /// returns [`crate::ErrorKind::NotFound`]. Only concrete ETag values
    /// are portable; a wildcard such as `"*"` has no portable meaning here.
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::read_with_if_none_match`].
    pub if_none_match: Option<String>,
    /// Read only when the file has this exact version.
    ///
    /// The condition succeeds when the file exists and its version equals
    /// this value. A file with a different version returns
    /// [`crate::ErrorKind::ConditionNotMatch`]; a missing file returns
    /// [`crate::ErrorKind::NotFound`].
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::read_with_if_version_match`].
    pub if_version_match: Option<String>,
    /// Read only when the file does not have this version.
    ///
    /// The condition succeeds when the file exists with a different version.
    /// A file whose version equals this value returns
    /// [`crate::ErrorKind::ConditionNotMatch`]; a missing file returns
    /// [`crate::ErrorKind::NotFound`].
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::read_with_if_version_not_match`].
    pub if_version_not_match: Option<String>,
    /// Read only when the file was modified after this timestamp.
    ///
    /// The condition succeeds when the file exists and was modified after
    /// this time. A file not modified since this time returns
    /// [`crate::ErrorKind::ConditionNotMatch`]; a missing file returns
    /// [`crate::ErrorKind::NotFound`].
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::read_with_if_modified_since`].
    #[serde(deserialize_with = "de::option_timestamp_millis")]
    pub if_modified_since: Option<Timestamp>,
    /// Read only when the file was not modified after this timestamp.
    ///
    /// The condition succeeds when the file exists and was not modified
    /// after this time. A file modified after this time returns
    /// [`crate::ErrorKind::ConditionNotMatch`]; a missing file returns
    /// [`crate::ErrorKind::NotFound`].
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::read_with_if_unmodified_since`].
    #[serde(deserialize_with = "de::option_timestamp_millis")]
    pub if_unmodified_since: Option<Timestamp>,

    /// Known content length of the object.
    ///
    /// This is an execution hint that allows OpenDAL to avoid extra metadata
    /// requests while planning reads. It must not be used as an object identity
    /// or consistency condition.
    pub content_length_hint: Option<u64>,

    /// Set `concurrent` for the operation.
    ///
    /// OpenDAL by default to read file without concurrent. This is not efficient for cases when users
    /// read large chunks of data. By setting `concurrent`, opendal will reading files concurrently
    /// on support storage services.
    ///
    /// By setting `concurrent`, opendal will fetch chunks concurrently with
    /// the give chunk size.
    ///
    /// Refer to the [performance guide](https://github.com/apache/opendal/tree/main/core/core/src/docs/performance)
    /// for more details.
    pub concurrent: usize,
    /// Set `chunk` for the operation.
    ///
    /// OpenDAL will use services' preferred chunk size by default. Users can set chunk based on their own needs.
    ///
    /// Refer to the [performance guide](https://github.com/apache/opendal/tree/main/core/core/src/docs/performance)
    /// for more details.
    pub chunk: Option<usize>,
    /// Controls the optimization strategy for range reads in [`crate::Reader::fetch`].
    ///
    /// When performing range reads, if the gap between two requested ranges is less than or
    /// equal to the configured `gap` size, OpenDAL will merge these ranges into a single read request
    /// and discard the unrequested data in between. This helps reduce the number of API calls
    /// to remote storage services.
    ///
    /// Set to `0` to disable merging ranges separated by a gap. Overlapping or adjacent ranges
    /// are still merged.
    ///
    /// This optimization is particularly useful when performing multiple small range reads
    /// that are close to each other, as it reduces the overhead of multiple network requests
    /// at the cost of transferring some additional data.
    ///
    /// Refer to the [performance guide](https://github.com/apache/opendal/tree/main/core/core/src/docs/performance)
    /// for more details.
    pub gap: Option<usize>,
    /// Controls the number of prefetched bytes ranges that can be buffered in memory
    /// during concurrent reading.
    ///
    /// When performing concurrent reads with `Reader`, this option limits how many
    /// completed-but-not-yet-read chunks can be buffered. Once the number of buffered
    /// chunks reaches this limit, no new read tasks will be spawned until some of the
    /// buffered chunks are consumed.
    ///
    /// - Default value: 0 (no prefetching, strict back-pressure control)
    /// - Set to a higher value to allow more aggressive prefetching at the cost of memory
    ///
    /// This option helps prevent memory exhaustion when reading large files with high
    /// concurrency settings.
    pub prefetch: usize,
}

/// Options for stat operations.
///
/// Each condition checks the file being observed, and every condition must
/// hold for the stat to return metadata. A missing file fails the stat with
/// [`crate::ErrorKind::NotFound`] no matter which conditions are set. An
/// existing file that fails a condition returns
/// [`crate::ErrorKind::ConditionNotMatch`], and a condition the service does
/// not advertise through its capability returns
/// [`crate::ErrorKind::Unsupported`]; OpenDAL never silently drops a
/// condition.
///
/// See the [conditional operation specification][crate::docs::specs::conditional_operations]
/// for the complete cross-operation contract.
#[derive(Debug, Clone, Default, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StatOptions {
    /// Stat the given version of the file instead of the current one.
    ///
    /// This selects which stored version the stat describes; it is not a
    /// condition on the file. A version that does not exist returns
    /// [`crate::ErrorKind::NotFound`].
    ///
    /// Check [`crate::Capability::stat_with_version`] before using this
    /// option.
    pub version: Option<String>,

    /// Stat only when the file has this exact ETag.
    ///
    /// The condition succeeds when the file exists and its ETag equals this
    /// value. A file with a different ETag returns
    /// [`crate::ErrorKind::ConditionNotMatch`]; a missing file returns
    /// [`crate::ErrorKind::NotFound`]. Only concrete ETag values are
    /// portable; a wildcard such as `"*"` has no portable meaning here.
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise [`crate::Capability::stat_with_if_match`].
    pub if_match: Option<String>,
    /// Stat only when the file does not have this ETag.
    ///
    /// With a concrete ETag value, the condition succeeds when the file
    /// exists with a different ETag. A file whose ETag equals this value
    /// returns [`crate::ErrorKind::ConditionNotMatch`]; a missing file
    /// returns [`crate::ErrorKind::NotFound`]. Only concrete ETag values
    /// are portable; a wildcard such as `"*"` has no portable meaning here.
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::stat_with_if_none_match`].
    pub if_none_match: Option<String>,
    /// Stat only when the file has this exact version.
    ///
    /// The condition succeeds when the file exists and its version equals
    /// this value. A file with a different version returns
    /// [`crate::ErrorKind::ConditionNotMatch`]; a missing file returns
    /// [`crate::ErrorKind::NotFound`].
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::stat_with_if_version_match`].
    pub if_version_match: Option<String>,
    /// Stat only when the file does not have this version.
    ///
    /// The condition succeeds when the file exists with a different version.
    /// A file whose version equals this value returns
    /// [`crate::ErrorKind::ConditionNotMatch`]; a missing file returns
    /// [`crate::ErrorKind::NotFound`].
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::stat_with_if_version_not_match`].
    pub if_version_not_match: Option<String>,
    /// Stat only when the file was modified after this timestamp.
    ///
    /// The condition succeeds when the file exists and was modified after
    /// this time. A file not modified since this time returns
    /// [`crate::ErrorKind::ConditionNotMatch`]; a missing file returns
    /// [`crate::ErrorKind::NotFound`].
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::stat_with_if_modified_since`].
    #[serde(deserialize_with = "de::option_timestamp_millis")]
    pub if_modified_since: Option<Timestamp>,
    /// Stat only when the file was not modified after this timestamp.
    ///
    /// The condition succeeds when the file exists and was not modified
    /// after this time. A file modified after this time returns
    /// [`crate::ErrorKind::ConditionNotMatch`]; a missing file returns
    /// [`crate::ErrorKind::NotFound`].
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::stat_with_if_unmodified_since`].
    #[serde(deserialize_with = "de::option_timestamp_millis")]
    pub if_unmodified_since: Option<Timestamp>,

    /// Specify the content-type header that should be sent back by the operation.
    ///
    /// This option is only meaningful when used along with presign.
    pub override_content_type: Option<String>,
    /// Specify the `cache-control` header that should be sent back by the operation.
    ///
    /// This option is only meaningful when used along with presign.
    pub override_cache_control: Option<String>,
    /// Specify the `content-disposition` header that should be sent back by the operation.
    ///
    /// This option is only meaningful when used along with presign.
    pub override_content_disposition: Option<String>,
}

/// Options for write operations.
///
/// Each condition checks the file currently stored at the write path, and
/// the service evaluates it atomically with the write's visible commit:
///
/// - A false condition fails the write with
///   [`crate::ErrorKind::ConditionNotMatch`] and leaves the previously
///   visible file unchanged. Depending on the service, the error may surface
///   when the write starts, while writing, or when closing the writer.
/// - A condition the service does not advertise through its capability fails
///   the write with [`crate::ErrorKind::Unsupported`]; OpenDAL never
///   silently drops a condition. A service that cannot preserve a
///   combination of conditions also returns `Unsupported`.
/// - A service-side conflict unrelated to these conditions surfaces as
///   [`crate::ErrorKind::Conflict`].
///
/// See the [conditional operation specification][crate::docs::specs::conditional_operations]
/// for the complete cross-operation contract.
#[derive(Debug, Clone, Default, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WriteOptions {
    /// Sets append mode for this operation.
    ///
    /// ### Capability
    ///
    /// Check [`crate::Capability::write_can_append`] before using this option.
    ///
    /// ### Behavior
    ///
    /// - By default, write operations overwrite existing files
    /// - When append is set to true:
    ///   - New data will be appended to the end of existing file
    ///   - If file doesn't exist, it will be created
    /// - If not supported, will return an error
    ///
    /// This operation allows adding data to existing files instead of overwriting them.
    pub append: bool,

    /// Sets Cache-Control header for this write operation.
    ///
    /// ### Capability
    ///
    /// Check [`crate::Capability::write_with_cache_control`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, sets Cache-Control as system metadata on the target file
    /// - The value should follow HTTP Cache-Control header format
    /// - If not supported, the value will be ignored
    ///
    /// This operation allows controlling caching behavior for the written content.
    ///
    /// ### Use Cases
    ///
    /// - Setting browser cache duration
    /// - Configuring CDN behavior
    /// - Optimizing content delivery
    /// - Managing cache invalidation
    ///
    /// ### References
    ///
    /// - [MDN Cache-Control](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control)
    /// - [RFC 7234 Section 5.2](https://tools.ietf.org/html/rfc7234#section-5.2)
    pub cache_control: Option<String>,
    /// Sets `Content-Type` header for this write operation.
    ///
    /// ## Capability
    ///
    /// Check [`crate::Capability::write_with_content_type`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, sets Content-Type as system metadata on the target file
    /// - The value should follow MIME type format (e.g. "text/plain", "image/jpeg")
    /// - If not supported, the value will be ignored
    ///
    /// This operation allows specifying the media type of the content being written.
    pub content_type: Option<String>,
    /// Sets Content-Disposition header for this write request.
    ///
    /// ### Capability
    ///
    /// Check [`crate::Capability::write_with_content_disposition`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, sets Content-Disposition as system metadata on the target file
    /// - The value should follow HTTP Content-Disposition header format
    /// - Common values include:
    ///   - `inline` - Content displayed within browser
    ///   - `attachment` - Content downloaded as file
    ///   - `attachment; filename="example.jpg"` - Downloaded with specified filename
    /// - If not supported, the value will be ignored
    ///
    /// This operation allows controlling how the content should be displayed or downloaded.
    pub content_disposition: Option<String>,
    /// Sets Content-Encoding header for this write request.
    ///
    /// ### Capability
    ///
    /// Check [`crate::Capability::write_with_content_encoding`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, sets Content-Encoding as system metadata on the target file
    /// - The value should follow HTTP Content-Encoding header format
    /// - Common values include:
    ///   - `gzip` - Content encoded using gzip compression
    ///   - `deflate` - Content encoded using deflate compression
    ///   - `br` - Content encoded using Brotli compression
    ///   - `identity` - No encoding applied (default value)
    /// - If not supported, the value will be ignored
    ///
    /// This operation allows specifying the encoding applied to the content being written.
    pub content_encoding: Option<String>,
    /// Sets user metadata for this write request.
    ///
    /// ### Capability
    ///
    /// Check [`crate::Capability::write_with_user_metadata`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, the user metadata will be attached to the object during write
    /// - Accepts key-value pairs where both key and value are strings
    /// - Keys are case-insensitive in most services
    /// - Services may have limitations for user metadata, for example:
    ///   - Key length is typically limited (e.g., 1024 bytes)
    ///   - Value length is typically limited (e.g., 4096 bytes)
    ///   - Total metadata size might be limited
    ///   - Some characters might be forbidden in keys
    /// - If not supported, the metadata will be ignored
    ///
    /// User metadata provides a way to attach custom metadata to objects during write operations.
    /// This metadata can be retrieved later when reading the object.
    pub user_metadata: Option<HashMap<String, String>>,

    /// Write only when the file at the write path has this exact ETag.
    ///
    /// The condition succeeds when the file exists and its ETag equals this
    /// value. A file with a different ETag, or a missing file, fails the
    /// write with [`crate::ErrorKind::ConditionNotMatch`]. Only concrete
    /// ETag values are portable; a wildcard such as `"*"` has no portable
    /// meaning here.
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise [`crate::Capability::write_with_if_match`].
    pub if_match: Option<String>,
    /// Write only when the file at the write path does not have this ETag.
    ///
    /// With a concrete ETag value, the condition succeeds when the file has
    /// a different ETag or does not exist; the write then replaces or
    /// creates the file. A file whose ETag equals this value fails the write
    /// with [`crate::ErrorKind::ConditionNotMatch`]. Only concrete ETag
    /// values are portable; a wildcard such as `"*"` has no portable meaning
    /// here.
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::write_with_if_none_match`]. Some services, such
    /// as `s3`, support `if_not_exists` but not `if_none_match`; use
    /// [`WriteOptions::if_not_exists`] when you only need to guard against
    /// an existing file.
    pub if_none_match: Option<String>,
    /// Write only when the file at the write path has this exact version.
    ///
    /// The condition succeeds when the file exists and its version equals
    /// this value. A file with a different version, or a missing file, fails
    /// the write with [`crate::ErrorKind::ConditionNotMatch`].
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::write_with_if_version_match`].
    pub if_version_match: Option<String>,
    /// Write only when the file at the write path does not have this
    /// version.
    ///
    /// The condition succeeds when the file exists with a different version.
    /// A file whose version equals this value fails the write with
    /// [`crate::ErrorKind::ConditionNotMatch`]. When no file exists at the
    /// write path, no portable behavior is defined.
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::write_with_if_version_not_match`].
    pub if_version_not_match: Option<String>,
    /// Write only when no file exists at the write path.
    ///
    /// The condition succeeds when the path is empty, so the write creates
    /// the file. An existing file fails the write with
    /// [`crate::ErrorKind::ConditionNotMatch`].
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::write_with_if_not_exists`].
    pub if_not_exists: bool,
    /// Write only when the file at the write path still has the identity
    /// recorded in this metadata.
    ///
    /// Pass metadata previously returned by OpenDAL for the same path.
    /// OpenDAL derives a version match when the service supports version
    /// conditions and the metadata contains a version. Otherwise it derives
    /// an ETag match when possible. A changed or missing file fails the write
    /// with [`crate::ErrorKind::ConditionNotMatch`], as does combining this
    /// option with a conflicting `if_match` or `if_version_match` value.
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not support the derived primitive condition, and
    /// [`crate::ErrorKind::ConfigInvalid`] when the metadata contains neither
    /// a version nor an ETag.
    #[serde(deserialize_with = "de::option_metadata_identity")]
    pub if_not_changed: Option<Metadata>,

    /// Sets concurrent write operations for this writer.
    ///
    /// ## Behavior
    ///
    /// - By default, OpenDAL writes files sequentially
    /// - When concurrent is set:
    ///   - Multiple write operations can execute in parallel
    ///   - Write operations return immediately without waiting if tasks space are available
    ///   - Close operation ensures all writes complete in order
    ///   - Memory usage increases with concurrency level
    /// - If not supported, falls back to sequential writes
    ///
    /// This feature significantly improves performance when:
    /// - Writing large files
    /// - Network latency is high
    /// - Storage service supports concurrent uploads like multipart uploads
    ///
    /// ## Performance Impact
    ///
    /// Setting appropriate concurrency can:
    /// - Increase write throughput
    /// - Reduce total write time
    /// - Better utilize available bandwidth
    /// - Trade memory for performance
    pub concurrent: usize,
    /// Sets chunk size for buffered writes.
    ///
    /// ### Capability
    ///
    /// Check [`crate::Capability::write_multi_min_size`] and [`crate::Capability::write_multi_max_size`] for size limits.
    ///
    /// ### Behavior
    ///
    /// - By default, OpenDAL sets optimal chunk size based on service capabilities
    /// - When chunk size is set:
    ///   - Data will be buffered until reaching chunk size
    ///   - One API call will be made per chunk
    ///   - Last chunk may be smaller than chunk size
    /// - Important considerations:
    ///   - Some services require minimum chunk sizes (e.g. S3's EntityTooSmall error)
    ///   - Smaller chunks increase API calls and costs
    ///   - Larger chunks increase memory usage, but improve performance and reduce costs
    ///
    /// ### Performance Impact
    ///
    /// Setting appropriate chunk size can:
    /// - Reduce number of API calls
    /// - Improve overall throughput
    /// - Lower operation costs
    /// - Better utilize network bandwidth
    pub chunk: Option<usize>,
}

/// Options for one complete source object in a composition.
///
/// These options select or constrain the source object. They do not apply to
/// the destination object.
#[derive(Debug, Clone, Default, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ComposeSourceOptions {
    /// Compose this version of the source object.
    ///
    /// This selects which stored version the composition reads; it is not a
    /// condition on the destination object.
    ///
    /// Check [`crate::Capability::compose_with_source_version`] before using
    /// this option.
    pub version: Option<String>,
    /// Compose only when the selected source has this exact ETag.
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::compose_with_source_if_match`].
    pub if_match: Option<String>,
    /// Compose only when the selected source retains this metadata identity.
    ///
    /// OpenDAL selects the metadata version when the service supports source
    /// versions. Otherwise it requires the ETag when possible. The operation
    /// returns [`crate::ErrorKind::Unsupported`] when the service does not
    /// support the derived source option, and
    /// [`crate::ErrorKind::ConfigInvalid`] when the metadata contains neither
    /// identity.
    #[serde(deserialize_with = "de::option_metadata_identity")]
    pub if_not_changed: Option<Metadata>,
}

/// Options for composing complete source objects into one destination object.
///
/// Metadata fields apply only to the destination. Conditions check the
/// destination object and follow the matching [`WriteOptions`] contracts.
#[derive(Debug, Clone, Default, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ComposeOptions {
    /// Sets Cache-Control metadata on the destination object.
    pub cache_control: Option<String>,
    /// Sets Content-Type metadata on the destination object.
    pub content_type: Option<String>,
    /// Sets Content-Disposition metadata on the destination object.
    pub content_disposition: Option<String>,
    /// Sets Content-Encoding metadata on the destination object.
    pub content_encoding: Option<String>,
    /// Sets user metadata on the destination object.
    pub user_metadata: Option<HashMap<String, String>>,
    /// Compose only when the destination has this exact ETag.
    pub if_match: Option<String>,
    /// Compose only when the destination does not have this ETag.
    pub if_none_match: Option<String>,
    /// Compose only when the destination has this exact version.
    pub if_version_match: Option<String>,
    /// Compose only when the destination does not have this version.
    pub if_version_not_match: Option<String>,
    /// Compose only when no destination object exists.
    pub if_not_exists: bool,
    /// Compose only when the destination still has this metadata identity.
    ///
    /// OpenDAL derives a version match when the service supports version
    /// conditions and the metadata contains a version. Otherwise it derives
    /// an ETag match when possible. The operation returns
    /// [`crate::ErrorKind::Unsupported`] when the service does not support the
    /// derived primitive condition, and [`crate::ErrorKind::ConfigInvalid`]
    /// when the metadata contains neither a version nor an ETag.
    #[serde(deserialize_with = "de::option_metadata_identity")]
    pub if_not_changed: Option<Metadata>,
    /// Maximum number of independent backend composition tasks.
    ///
    /// The default value is `1`. Services that use one atomic request may
    /// ignore values greater than `1`.
    pub concurrent: usize,
}

/// Options for copy operations.
///
/// Each condition checks the destination file, never the source, and every
/// condition must hold for the copy to proceed:
///
/// - A false condition fails the copy with
///   [`crate::ErrorKind::ConditionNotMatch`].
/// - A condition the service does not advertise through its capability fails
///   the copy with [`crate::ErrorKind::Unsupported`]; OpenDAL never silently
///   drops a condition.
/// - A missing source fails the copy with [`crate::ErrorKind::NotFound`];
///   when a condition error applies at the same time, which error the copy
///   returns is unspecified.
/// - A service-side conflict unrelated to these conditions surfaces as
///   [`crate::ErrorKind::Conflict`].
///
/// See the [conditional operation specification][crate::docs::specs::conditional_operations]
/// for the complete cross-operation contract.
#[derive(Debug, Clone, Default, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CopyOptions {
    /// Copy only when no file exists at the destination path.
    ///
    /// The condition succeeds when the destination path is empty, so the
    /// copy creates the destination file. An existing destination file fails
    /// the copy with [`crate::ErrorKind::ConditionNotMatch`].
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::copy_with_if_not_exists`].
    pub if_not_exists: bool,

    /// Copy only when the destination file has this exact ETag.
    ///
    /// The condition succeeds when the destination file exists and its ETag
    /// equals this value. A destination file with a different ETag, or a
    /// missing destination file, fails the copy with
    /// [`crate::ErrorKind::ConditionNotMatch`]. Only concrete ETag values
    /// are portable; a wildcard such as `"*"` has no portable meaning here.
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise [`crate::Capability::copy_with_if_match`].
    pub if_match: Option<String>,
    /// Copy only when the destination file does not have this ETag.
    ///
    /// With a concrete ETag value, the condition succeeds when the
    /// destination file has a different ETag or does not exist; the copy
    /// then replaces or creates the destination file. A destination file
    /// whose ETag equals this value fails the copy with
    /// [`crate::ErrorKind::ConditionNotMatch`]. Only concrete ETag values
    /// are portable; a wildcard such as `"*"` has no portable meaning here.
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::copy_with_if_none_match`].
    pub if_none_match: Option<String>,
    /// Copy only when the destination file has this exact version.
    ///
    /// The condition succeeds when the destination file exists and its
    /// version equals this value. A destination file with a different
    /// version, or a missing destination file, fails the copy with
    /// [`crate::ErrorKind::ConditionNotMatch`].
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::copy_with_if_version_match`].
    pub if_version_match: Option<String>,
    /// Copy only when the destination file does not have this version.
    ///
    /// The condition succeeds when the destination file exists with a
    /// different version. A destination file whose version equals this value
    /// fails the copy with [`crate::ErrorKind::ConditionNotMatch`]. When no
    /// destination file exists, no portable behavior is defined.
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::copy_with_if_version_not_match`].
    pub if_version_not_match: Option<String>,
    /// Copy only when the destination file still has the identity recorded
    /// in this metadata.
    ///
    /// Pass metadata previously returned by OpenDAL for the destination
    /// path. OpenDAL derives a version match when the service supports version
    /// conditions and the metadata contains a version. Otherwise it derives
    /// an ETag match when possible. A changed or missing destination file
    /// fails the copy with [`crate::ErrorKind::ConditionNotMatch`], as does
    /// combining this option with a conflicting `if_match` or
    /// `if_version_match` value.
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not support the derived primitive condition, and
    /// [`crate::ErrorKind::ConfigInvalid`] when the metadata contains neither
    /// a version nor an ETag.
    #[serde(deserialize_with = "de::option_metadata_identity")]
    pub if_not_changed: Option<Metadata>,

    /// Copy from a specific source file version.
    ///
    /// This selects which stored version of the source the copy reads; it is
    /// not a condition on the destination file. Destination behavior follows
    /// normal copy semantics.
    ///
    /// Check [`crate::Capability::copy_with_source_version`] before using
    /// this option.
    pub source_version: Option<String>,

    /// Asserted complete content length of the source object.
    ///
    /// OpenDAL may trust this value without reading source metadata. It can use
    /// the value to plan copied ranges and to construct the returned metadata.
    /// An incorrect value can therefore cause incomplete copy planning, failed
    /// requests, or incorrect result metadata.
    ///
    /// A service can ignore this value when its copy operation already reports
    /// an authoritative copied size.
    ///
    /// This option does not pin the source object or protect the copy from a
    /// concurrent source change. Set it only when the caller can guarantee that
    /// the value describes the object that the service will copy, for example by
    /// selecting an immutable [`Self::source_version`]. Otherwise omit it if the
    /// source can change.
    pub source_content_length_hint: Option<u64>,

    /// Sets concurrent copy operations for this copier.
    ///
    /// This is a best-effort execution option. Services that cannot split copy
    /// into concurrent server-side tasks can ignore it.
    pub concurrent: usize,

    /// Sets chunk size for segmented copy operations.
    ///
    /// ### Capability
    ///
    /// Check [`crate::Capability::copy_can_multi`],
    /// [`crate::Capability::copy_multi_min_size`] and
    /// [`crate::Capability::copy_multi_max_size`] before using this feature.
    ///
    /// This is a best-effort execution option. Services that support
    /// server-side segmented copy can use it as the target size for each copy
    /// step. Services that cannot split copy operations can ignore it.
    pub chunk: Option<usize>,
}

/// Options for rename operations.
///
/// The condition checks the destination file, never the source. A missing
/// source fails the rename with [`crate::ErrorKind::NotFound`]; when a
/// condition error applies at the same time, which error the rename returns
/// is unspecified.
///
/// See the [conditional operation specification][crate::docs::specs::conditional_operations]
/// for the complete cross-operation contract.
#[derive(Debug, Clone, Default, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RenameOptions {
    /// Rename only when no file exists at the destination path.
    ///
    /// The condition succeeds when the destination path is empty. An
    /// existing destination file fails the rename with
    /// [`crate::ErrorKind::ConditionNotMatch`].
    ///
    /// The operation returns [`crate::ErrorKind::Unsupported`] when the
    /// service does not advertise
    /// [`crate::Capability::rename_with_if_not_exists`].
    pub if_not_exists: bool,
}

/// Options for restore operations.
///
/// The condition checks the file currently stored at the restore path. The
/// selected version remains the source of the restore, and the restore may
/// still fail because that version does not exist.
///
/// See the [conditional operation specification][crate::docs::specs::conditional_operations]
/// for the complete cross-operation contract.
#[derive(Debug, Clone, Default, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RestoreOptions {
    /// Restore this historical version as the current version.
    ///
    /// This selects which stored version the restore promotes; it is not a
    /// condition on the file at the restore path.
    ///
    /// Check [`crate::Capability::restore_with_version`] before using this
    /// option.
    pub version: Option<String>,

    /// Restore the selected version only when no file currently exists at
    /// the restore path.
    ///
    /// The condition succeeds when the restore path is empty. An existing
    /// file fails the restore with [`crate::ErrorKind::ConditionNotMatch`],
    /// which protects recovery workflows from overwriting a file recreated
    /// after the version to restore was selected.
    ///
    /// This option requires [`RestoreOptions::version`]; setting it without
    /// a version returns [`crate::ErrorKind::ConfigInvalid`]. The operation
    /// returns [`crate::ErrorKind::Unsupported`] when the service does not
    /// advertise [`crate::Capability::restore_with_if_not_exists`].
    pub if_not_exists: bool,
}

/// Representations of the field types that have no serde format of their own.
mod de {
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::de::Error as _;

    use crate::Metadata;
    use crate::MetadataBuilder;
    use crate::raw::Timestamp;
    use crate::types::BytesRange;

    /// An optional timestamp as Unix milliseconds.
    pub(super) fn option_timestamp_millis<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Timestamp>, D::Error> {
        Option::<i64>::deserialize(deserializer)?
            .map(|millis| Timestamp::from_millisecond(millis).map_err(D::Error::custom))
            .transpose()
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct MetadataIdentity {
        etag: Option<String>,
        version: Option<String>,
    }

    /// Optional metadata given as the ETag and version that conditions
    /// derived from metadata compare.
    pub(super) fn option_metadata_identity<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Metadata>, D::Error> {
        Ok(
            Option::<MetadataIdentity>::deserialize(deserializer)?.map(|identity| {
                let mut builder = MetadataBuilder::unknown();
                if let Some(etag) = identity.etag {
                    builder.etag(etag);
                }
                if let Some(version) = identity.version {
                    builder.version(version);
                }
                builder.build()
            }),
        )
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct RangeRepr {
        offset: Option<u64>,
        size: Option<u64>,
        suffix: Option<u64>,
    }

    /// A byte range given as `{"offset", "size"}` or `{"suffix"}`.
    pub(super) fn bytes_range<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BytesRange, D::Error> {
        let range = RangeRepr::deserialize(deserializer)?;
        match (range.offset, range.size, range.suffix) {
            (None, None, Some(size)) => Ok(BytesRange::suffix(size)),
            (_, _, Some(_)) => Err(D::Error::custom(
                "`suffix` cannot be combined with `offset` or `size`",
            )),
            (offset, size, None) => {
                let offset = offset.unwrap_or_default();
                if let Some(size) = size
                    && offset.checked_add(size).is_none()
                {
                    return Err(D::Error::custom("`offset` + `size` overflows u64"));
                }
                Ok(BytesRange::new(offset, size))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_uses_defaults_for_missing_members() {
        let options: ReadOptions = serde_json::from_str("{}").unwrap();
        assert_eq!(options, ReadOptions::default());
    }

    #[test]
    fn deserialize_rejects_unknown_members() {
        let err = serde_json::from_str::<StatOptions>(r#"{"if_matc": "x"}"#)
            .expect_err("a misspelled option must fail");
        assert!(err.to_string().contains("unknown field"), "{err}");
    }

    #[test]
    fn deserialize_reads_plain_members() {
        let options: WriteOptions = serde_json::from_str(
            r#"{"append": true, "content_type": "text/plain", "user_metadata": {"k": "v"}, "concurrent": 4, "chunk": 1024}"#,
        )
        .unwrap();
        assert!(options.append);
        assert_eq!(options.content_type.as_deref(), Some("text/plain"));
        assert_eq!(options.user_metadata.unwrap()["k"], "v");
        assert_eq!(options.concurrent, 4);
        assert_eq!(options.chunk, Some(1024));
    }

    #[test]
    fn deserialize_reads_timestamps_as_unix_milliseconds() {
        let options: StatOptions =
            serde_json::from_str(r#"{"if_modified_since": 1700000000123}"#).unwrap();
        assert_eq!(
            options.if_modified_since,
            Some(Timestamp::from_millisecond(1700000000123).unwrap())
        );
    }

    #[test]
    fn deserialize_reads_range_forms() {
        let range = |json: &str| serde_json::from_str::<ReadOptions>(json).map(|o| o.range);

        assert_eq!(
            range(r#"{"range": {"offset": 10, "size": 100}}"#).unwrap(),
            BytesRange::new(10, Some(100))
        );
        assert_eq!(
            range(r#"{"range": {"offset": 10}}"#).unwrap(),
            BytesRange::new(10, None)
        );
        assert_eq!(
            range(r#"{"range": {"suffix": 100}}"#).unwrap(),
            BytesRange::suffix(100)
        );
        assert!(range(r#"{"range": {"offset": 1, "suffix": 100}}"#).is_err());
        assert!(range(r#"{"range": {"offset": 18446744073709551615, "size": 1}}"#).is_err());
        assert!(range(r#"{"range": {"length": 1}}"#).is_err());
    }

    #[test]
    fn deserialize_reads_if_not_changed_identity() {
        let options: DeleteOptions =
            serde_json::from_str(r#"{"if_not_changed": {"etag": "\"abc\"", "version": "v1"}}"#)
                .unwrap();
        let metadata = options.if_not_changed.unwrap();
        assert_eq!(metadata.etag(), Some("\"abc\""));
        assert_eq!(metadata.version(), Some("v1"));

        // An object with neither member must not drop the condition.
        let options: CopyOptions = serde_json::from_str(r#"{"if_not_changed": {}}"#).unwrap();
        let metadata = options.if_not_changed.unwrap();
        assert_eq!(metadata.etag(), None);
        assert_eq!(metadata.version(), None);
    }
}
