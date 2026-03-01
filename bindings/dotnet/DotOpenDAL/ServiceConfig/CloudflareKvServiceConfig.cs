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
    public sealed class CloudflareKvServiceConfig : IServiceConfig
    {
        public string? AccountId { get; init; }
        public string? ApiToken { get; init; }
        public string? DefaultTtl { get; init; }
        public string? NamespaceId { get; init; }
        public string? Root { get; init; }

        public string Scheme => "cloudflare_kv";

        public IReadOnlyDictionary<string, string> ToOptions()
        {
            var map = new Dictionary<string, string>();
            if (AccountId is not null)
            {
                map["account_id"] = Utilities.ToOptionString(AccountId);
            }
            if (ApiToken is not null)
            {
                map["api_token"] = Utilities.ToOptionString(ApiToken);
            }
            if (DefaultTtl is not null)
            {
                map["default_ttl"] = Utilities.ToOptionString(DefaultTtl);
            }
            if (NamespaceId is not null)
            {
                map["namespace_id"] = Utilities.ToOptionString(NamespaceId);
            }
            if (Root is not null)
            {
                map["root"] = Utilities.ToOptionString(Root);
            }
            return map;
        }
    }

}
