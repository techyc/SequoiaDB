﻿/* Copyright 2010-2012 10gen Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;

namespace SqlDB.Bson.Serialization.Conventions
{
    /// <summary>
    /// Represents a serialize default value convention.
    /// </summary>
    [Obsolete("ISerializeDefaultValueConvention is obsolete and will be removed in a future version of the C# driver. Please use IIgnoreIfDefaultConvention instead.")]
    public interface ISerializeDefaultValueConvention
    {
        /// <summary>
        /// Determines whether to serialize the default value for a member.
        /// </summary>
        /// <param name="memberInfo">The member.</param>
        /// <returns>Whether to serialize the default value.</returns>
        bool SerializeDefaultValue(MemberInfo memberInfo);
    }

    /// <summary>
    /// Represents a serialize default value convention where default values are never serialized.
    /// </summary>
    [Obsolete("NeverSerializeDefaultValueConvention is obsolete and will be removed in a future version of the C# driver. Please use AlwaysIgnoreIfDefaultConvention instead.")]
    public class NeverSerializeDefaultValueConvention : ISerializeDefaultValueConvention
    {
        /// <summary>
        /// Determines whether to serialize the default value for a member.
        /// </summary>
        /// <param name="memberInfo">The member.</param>
        /// <returns>Whether to serialize the default value.</returns>
        public bool SerializeDefaultValue(MemberInfo memberInfo)
        {
            return false;
        }
    }

    /// <summary>
    /// Represents a serialize default value convention where default values are always serialized.
    /// </summary>
    [Obsolete("AlwaysSerializeDefaultValueConvention is obsolete and will be removed in a future version of the C# driver. Please use NeverIgnoreIfDefaultConvention instead.")]
    public class AlwaysSerializeDefaultValueConvention : ISerializeDefaultValueConvention
    {
        /// <summary>
        /// Determines whether to serialize the default value for a member.
        /// </summary>
        /// <param name="memberInfo">The member.</param>
        /// <returns>Whether to serialize the default value.</returns>
        public bool SerializeDefaultValue(MemberInfo memberInfo)
        {
            return true;
        }
    }
}
