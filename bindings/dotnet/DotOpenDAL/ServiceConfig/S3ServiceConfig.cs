/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Generated from bindings/java ServiceConfig definitions.

using DotOpenDAL.ServiceConfig.Abstractions;

namespace DotOpenDAL.ServiceConfig
{
    public sealed class S3ServiceConfig : IServiceConfig
    {
        public string? AccessKeyId { get; init; }
        public bool? AllowAnonymous { get; init; }
        public long? BatchMaxOperations { get; init; }
        public string? Bucket { get; init; }
        public string? ChecksumAlgorithm { get; init; }
        public string? DefaultStorageClass { get; init; }
        public long? DeleteMaxSize { get; init; }
        public bool? DisableConfigLoad { get; init; }
        public bool? DisableEc2Metadata { get; init; }
        public bool? DisableListObjectsV2 { get; init; }
        public bool? DisableStatWithOverride { get; init; }
        public bool? DisableWriteWithIfMatch { get; init; }
        public bool? EnableRequestPayer { get; init; }
        public bool? EnableVersioning { get; init; }
        public bool? EnableVirtualHostStyle { get; init; }
        public bool? EnableWriteWithAppend { get; init; }
        public string? Endpoint { get; init; }
        public string? ExternalId { get; init; }
        public string? Region { get; init; }
        public string? RoleArn { get; init; }
        public string? RoleSessionName { get; init; }
        public string? Root { get; init; }
        public string? SecretAccessKey { get; init; }
        public string? ServerSideEncryption { get; init; }
        public string? ServerSideEncryptionAwsKmsKeyId { get; init; }
        public string? ServerSideEncryptionCustomerAlgorithm { get; init; }
        public string? ServerSideEncryptionCustomerKey { get; init; }
        public string? ServerSideEncryptionCustomerKeyMd5 { get; init; }
        public string? SessionToken { get; init; }

        public string Scheme => "s3";

        public IReadOnlyDictionary<string, string> ToOptions()
        {
            var map = new Dictionary<string, string>();
            if (AccessKeyId is not null)
            {
                map["access_key_id"] = Utilities.ToOptionString(AccessKeyId);
            }
            if (AllowAnonymous is not null)
            {
                map["allow_anonymous"] = Utilities.ToOptionString(AllowAnonymous);
            }
            if (BatchMaxOperations is not null)
            {
                map["batch_max_operations"] = Utilities.ToOptionString(BatchMaxOperations);
            }
            if (Bucket is not null)
            {
                map["bucket"] = Utilities.ToOptionString(Bucket);
            }
            if (ChecksumAlgorithm is not null)
            {
                map["checksum_algorithm"] = Utilities.ToOptionString(ChecksumAlgorithm);
            }
            if (DefaultStorageClass is not null)
            {
                map["default_storage_class"] = Utilities.ToOptionString(DefaultStorageClass);
            }
            if (DeleteMaxSize is not null)
            {
                map["delete_max_size"] = Utilities.ToOptionString(DeleteMaxSize);
            }
            if (DisableConfigLoad is not null)
            {
                map["disable_config_load"] = Utilities.ToOptionString(DisableConfigLoad);
            }
            if (DisableEc2Metadata is not null)
            {
                map["disable_ec2_metadata"] = Utilities.ToOptionString(DisableEc2Metadata);
            }
            if (DisableListObjectsV2 is not null)
            {
                map["disable_list_objects_v2"] = Utilities.ToOptionString(DisableListObjectsV2);
            }
            if (DisableStatWithOverride is not null)
            {
                map["disable_stat_with_override"] = Utilities.ToOptionString(DisableStatWithOverride);
            }
            if (DisableWriteWithIfMatch is not null)
            {
                map["disable_write_with_if_match"] = Utilities.ToOptionString(DisableWriteWithIfMatch);
            }
            if (EnableRequestPayer is not null)
            {
                map["enable_request_payer"] = Utilities.ToOptionString(EnableRequestPayer);
            }
            if (EnableVersioning is not null)
            {
                map["enable_versioning"] = Utilities.ToOptionString(EnableVersioning);
            }
            if (EnableVirtualHostStyle is not null)
            {
                map["enable_virtual_host_style"] = Utilities.ToOptionString(EnableVirtualHostStyle);
            }
            if (EnableWriteWithAppend is not null)
            {
                map["enable_write_with_append"] = Utilities.ToOptionString(EnableWriteWithAppend);
            }
            if (Endpoint is not null)
            {
                map["endpoint"] = Utilities.ToOptionString(Endpoint);
            }
            if (ExternalId is not null)
            {
                map["external_id"] = Utilities.ToOptionString(ExternalId);
            }
            if (Region is not null)
            {
                map["region"] = Utilities.ToOptionString(Region);
            }
            if (RoleArn is not null)
            {
                map["role_arn"] = Utilities.ToOptionString(RoleArn);
            }
            if (RoleSessionName is not null)
            {
                map["role_session_name"] = Utilities.ToOptionString(RoleSessionName);
            }
            if (Root is not null)
            {
                map["root"] = Utilities.ToOptionString(Root);
            }
            if (SecretAccessKey is not null)
            {
                map["secret_access_key"] = Utilities.ToOptionString(SecretAccessKey);
            }
            if (ServerSideEncryption is not null)
            {
                map["server_side_encryption"] = Utilities.ToOptionString(ServerSideEncryption);
            }
            if (ServerSideEncryptionAwsKmsKeyId is not null)
            {
                map["server_side_encryption_aws_kms_key_id"] = Utilities.ToOptionString(ServerSideEncryptionAwsKmsKeyId);
            }
            if (ServerSideEncryptionCustomerAlgorithm is not null)
            {
                map["server_side_encryption_customer_algorithm"] = Utilities.ToOptionString(ServerSideEncryptionCustomerAlgorithm);
            }
            if (ServerSideEncryptionCustomerKey is not null)
            {
                map["server_side_encryption_customer_key"] = Utilities.ToOptionString(ServerSideEncryptionCustomerKey);
            }
            if (ServerSideEncryptionCustomerKeyMd5 is not null)
            {
                map["server_side_encryption_customer_key_md5"] = Utilities.ToOptionString(ServerSideEncryptionCustomerKeyMd5);
            }
            if (SessionToken is not null)
            {
                map["session_token"] = Utilities.ToOptionString(SessionToken);
            }
            return map;
        }
    }

}
