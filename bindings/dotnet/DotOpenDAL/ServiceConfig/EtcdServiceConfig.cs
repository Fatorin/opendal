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
    public sealed class EtcdServiceConfig : IServiceConfig
    {
        public string? CaPath { get; init; }
        public string? CertPath { get; init; }
        public string? Endpoints { get; init; }
        public string? KeyPath { get; init; }
        public string? Password { get; init; }
        public string? Root { get; init; }
        public string? Username { get; init; }

        public string Scheme => "etcd";

        public IReadOnlyDictionary<string, string> ToOptions()
        {
            var map = new Dictionary<string, string>();
            if (CaPath is not null)
            {
                map["ca_path"] = Utilities.ToOptionString(CaPath);
            }
            if (CertPath is not null)
            {
                map["cert_path"] = Utilities.ToOptionString(CertPath);
            }
            if (Endpoints is not null)
            {
                map["endpoints"] = Utilities.ToOptionString(Endpoints);
            }
            if (KeyPath is not null)
            {
                map["key_path"] = Utilities.ToOptionString(KeyPath);
            }
            if (Password is not null)
            {
                map["password"] = Utilities.ToOptionString(Password);
            }
            if (Root is not null)
            {
                map["root"] = Utilities.ToOptionString(Root);
            }
            if (Username is not null)
            {
                map["username"] = Utilities.ToOptionString(Username);
            }
            return map;
        }
    }

}
