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
    public sealed class GcsServiceConfig : IServiceConfig
    {
        public bool? AllowAnonymous { get; init; }
        public string? Bucket { get; init; }
        public string? Credential { get; init; }
        public string? CredentialPath { get; init; }
        public string? DefaultStorageClass { get; init; }
        public bool? DisableConfigLoad { get; init; }
        public bool? DisableVmMetadata { get; init; }
        public string? Endpoint { get; init; }
        public string? PredefinedAcl { get; init; }
        public string? Root { get; init; }
        public string? Scope { get; init; }
        public string? ServiceAccount { get; init; }
        public string? Token { get; init; }

        public string Scheme => "gcs";

        public IReadOnlyDictionary<string, string> ToOptions()
        {
            var map = new Dictionary<string, string>();
            if (AllowAnonymous is not null)
            {
                map["allow_anonymous"] = Utilities.ToOptionString(AllowAnonymous);
            }
            if (Bucket is not null)
            {
                map["bucket"] = Utilities.ToOptionString(Bucket);
            }
            if (Credential is not null)
            {
                map["credential"] = Utilities.ToOptionString(Credential);
            }
            if (CredentialPath is not null)
            {
                map["credential_path"] = Utilities.ToOptionString(CredentialPath);
            }
            if (DefaultStorageClass is not null)
            {
                map["default_storage_class"] = Utilities.ToOptionString(DefaultStorageClass);
            }
            if (DisableConfigLoad is not null)
            {
                map["disable_config_load"] = Utilities.ToOptionString(DisableConfigLoad);
            }
            if (DisableVmMetadata is not null)
            {
                map["disable_vm_metadata"] = Utilities.ToOptionString(DisableVmMetadata);
            }
            if (Endpoint is not null)
            {
                map["endpoint"] = Utilities.ToOptionString(Endpoint);
            }
            if (PredefinedAcl is not null)
            {
                map["predefined_acl"] = Utilities.ToOptionString(PredefinedAcl);
            }
            if (Root is not null)
            {
                map["root"] = Utilities.ToOptionString(Root);
            }
            if (Scope is not null)
            {
                map["scope"] = Utilities.ToOptionString(Scope);
            }
            if (ServiceAccount is not null)
            {
                map["service_account"] = Utilities.ToOptionString(ServiceAccount);
            }
            if (Token is not null)
            {
                map["token"] = Utilities.ToOptionString(Token);
            }
            return map;
        }
    }

}
