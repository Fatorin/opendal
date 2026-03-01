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
    public sealed class PostgresqlServiceConfig : IServiceConfig
    {
        public string? ConnectionString { get; init; }
        public string? KeyField { get; init; }
        public string? Root { get; init; }
        public string? Table { get; init; }
        public string? ValueField { get; init; }

        public string Scheme => "postgresql";

        public IReadOnlyDictionary<string, string> ToOptions()
        {
            var map = new Dictionary<string, string>();
            if (ConnectionString is not null)
            {
                map["connection_string"] = Utilities.ToOptionString(ConnectionString);
            }
            if (KeyField is not null)
            {
                map["key_field"] = Utilities.ToOptionString(KeyField);
            }
            if (Root is not null)
            {
                map["root"] = Utilities.ToOptionString(Root);
            }
            if (Table is not null)
            {
                map["table"] = Utilities.ToOptionString(Table);
            }
            if (ValueField is not null)
            {
                map["value_field"] = Utilities.ToOptionString(ValueField);
            }
            return map;
        }
    }

}
