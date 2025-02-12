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

using SqlDB.Bson.Serialization;
using SqlDB.Bson.Serialization.Options;

namespace SqlDB.Bson.Serialization.Attributes
{
    /// <summary>
    /// Specifies serialization options for a DateTime field or property.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class BsonDateTimeOptionsAttribute : BsonSerializationOptionsAttribute
    {
        // private fields
        private bool _dateOnly = false;
        private DateTimeKind _kind = DateTimeKind.Utc;
        private BsonType _representation = BsonType.DateTime;

        // constructors
        /// <summary>
        /// Initializes a new instance of the BsonDateTimeOptionsAttribute class.
        /// </summary>
        public BsonDateTimeOptionsAttribute()
        {
        }

        // public properties
        /// <summary>
        /// Gets or sets whether the DateTime consists of a Date only.
        /// </summary>
        public bool DateOnly
        {
            get { return _dateOnly; }
            set { _dateOnly = value; }
        }

        /// <summary>
        /// Gets or sets the DateTimeKind (Local, Unspecified or Utc).
        /// </summary>
        public DateTimeKind Kind
        {
            get { return _kind; }
            set { _kind = value; }
        }

        /// <summary>
        /// Gets or sets the external representation.
        /// </summary>
        public BsonType Representation
        {
            get { return _representation; }
            set { _representation = value; }
        }
    }
}
