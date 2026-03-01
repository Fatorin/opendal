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
    public sealed class OssServiceConfig : IServiceConfig
    {
        public string? AccessKeyId { get; init; }
        public string? AccessKeySecret { get; init; }
        public string? AddressingStyle { get; init; }
        public bool? AllowAnonymous { get; init; }
        public long? BatchMaxOperations { get; init; }
        public string? Bucket { get; init; }
        public long? DeleteMaxSize { get; init; }
        public bool? EnableVersioning { get; init; }
        public string? Endpoint { get; init; }
        public string? OidcProviderArn { get; init; }
        public string? OidcTokenFile { get; init; }
        public string? PresignAddressingStyle { get; init; }
        public string? PresignEndpoint { get; init; }
        public string? RoleArn { get; init; }
        public string? RoleSessionName { get; init; }
        public string? Root { get; init; }
        public string? SecurityToken { get; init; }
        public string? ServerSideEncryption { get; init; }
        public string? ServerSideEncryptionKeyId { get; init; }
        public string? StsEndpoint { get; init; }

        public string Scheme => "oss";

        public IReadOnlyDictionary<string, string> ToOptions()
        {
            var map = new Dictionary<string, string>();
            if (AccessKeyId is not null)
            {
                map["access_key_id"] = Utilities.ToOptionString(AccessKeyId);
            }
            if (AccessKeySecret is not null)
            {
                map["access_key_secret"] = Utilities.ToOptionString(AccessKeySecret);
            }
            if (AddressingStyle is not null)
            {
                map["addressing_style"] = Utilities.ToOptionString(AddressingStyle);
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
            if (DeleteMaxSize is not null)
            {
                map["delete_max_size"] = Utilities.ToOptionString(DeleteMaxSize);
            }
            if (EnableVersioning is not null)
            {
                map["enable_versioning"] = Utilities.ToOptionString(EnableVersioning);
            }
            if (Endpoint is not null)
            {
                map["endpoint"] = Utilities.ToOptionString(Endpoint);
            }
            if (OidcProviderArn is not null)
            {
                map["oidc_provider_arn"] = Utilities.ToOptionString(OidcProviderArn);
            }
            if (OidcTokenFile is not null)
            {
                map["oidc_token_file"] = Utilities.ToOptionString(OidcTokenFile);
            }
            if (PresignAddressingStyle is not null)
            {
                map["presign_addressing_style"] = Utilities.ToOptionString(PresignAddressingStyle);
            }
            if (PresignEndpoint is not null)
            {
                map["presign_endpoint"] = Utilities.ToOptionString(PresignEndpoint);
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
            if (SecurityToken is not null)
            {
                map["security_token"] = Utilities.ToOptionString(SecurityToken);
            }
            if (ServerSideEncryption is not null)
            {
                map["server_side_encryption"] = Utilities.ToOptionString(ServerSideEncryption);
            }
            if (ServerSideEncryptionKeyId is not null)
            {
                map["server_side_encryption_key_id"] = Utilities.ToOptionString(ServerSideEncryptionKeyId);
            }
            if (StsEndpoint is not null)
            {
                map["sts_endpoint"] = Utilities.ToOptionString(StsEndpoint);
            }
            return map;
        }
    }

}
