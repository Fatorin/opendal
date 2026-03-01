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
    public sealed class AzfileServiceConfig : IServiceConfig
    {
        public string? AccountKey { get; init; }
        public string? AccountName { get; init; }
        public string? Endpoint { get; init; }
        public string? Root { get; init; }
        public string? SasToken { get; init; }
        public string? ShareName { get; init; }

        public string Scheme => "azfile";

        public IReadOnlyDictionary<string, string> ToOptions()
        {
            var map = new Dictionary<string, string>();
            if (AccountKey is not null)
            {
                map["account_key"] = Utilities.ToOptionString(AccountKey);
            }
            if (AccountName is not null)
            {
                map["account_name"] = Utilities.ToOptionString(AccountName);
            }
            if (Endpoint is not null)
            {
                map["endpoint"] = Utilities.ToOptionString(Endpoint);
            }
            if (Root is not null)
            {
                map["root"] = Utilities.ToOptionString(Root);
            }
            if (SasToken is not null)
            {
                map["sas_token"] = Utilities.ToOptionString(SasToken);
            }
            if (ShareName is not null)
            {
                map["share_name"] = Utilities.ToOptionString(ShareName);
            }
            return map;
        }
    }

}
