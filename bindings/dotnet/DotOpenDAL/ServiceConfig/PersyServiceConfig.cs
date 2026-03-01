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
    public sealed class PersyServiceConfig : IServiceConfig
    {
        public string? Datafile { get; init; }
        public string? Index { get; init; }
        public string? Segment { get; init; }

        public string Scheme => "persy";

        public IReadOnlyDictionary<string, string> ToOptions()
        {
            var map = new Dictionary<string, string>();
            if (Datafile is not null)
            {
                map["datafile"] = Utilities.ToOptionString(Datafile);
            }
            if (Index is not null)
            {
                map["index"] = Utilities.ToOptionString(Index);
            }
            if (Segment is not null)
            {
                map["segment"] = Utilities.ToOptionString(Segment);
            }
            return map;
        }
    }

}
