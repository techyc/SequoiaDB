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
using System.Text.RegularExpressions;

namespace SqlDB.Bson
{
    /// <summary>
    /// Represents a BSON JavaScript value.
    /// </summary>
    [Serializable]
    public class BsonJavaScript : BsonValue, IComparable<BsonJavaScript>, IEquatable<BsonJavaScript>
    {
        // private fields
        private string _code;

        // constructors
        /// <summary>
        /// Initializes a new instance of the BsonJavaScript class.
        /// </summary>
        /// <param name="code">The JavaScript code.</param>
        public BsonJavaScript(string code)
            : base(BsonType.JavaScript)
        {
            if (code == null)
            {
                throw new ArgumentNullException("code");
            }
            _code = code;
        }

        /// <summary>
        /// Initializes a new instance of the BsonJavaScript class (only called by BsonJavaScriptWithScope).
        /// </summary>
        /// <param name="code">The JavaScript code.</param>
        /// <param name="bsonType">The BsonType (must be JavaScriptWithScope).</param>
        protected BsonJavaScript(string code, BsonType bsonType)
            : base(bsonType)
        {
            if (code == null)
            {
                throw new ArgumentNullException("code");
            }
            _code = code;
        }

        // public properties
        /// <summary>
        /// Gets the JavaScript code.
        /// </summary>
        public string Code
        {
            get { return _code; }
        }

        /// <summary>
        /// Compares two BsonJavaScript values.
        /// </summary>
        /// <param name="lhs">The first BsonJavaScript.</param>
        /// <param name="rhs">The other BsonJavaScript.</param>
        /// <returns>True if the two BsonJavaScript values are not equal according to ==.</returns>
        public static bool operator !=(BsonJavaScript lhs, BsonJavaScript rhs)
        {
            return !(lhs == rhs);
        }

        /// <summary>
        /// Compares two BsonJavaScript values.
        /// </summary>
        /// <param name="lhs">The first BsonJavaScript.</param>
        /// <param name="rhs">The other BsonJavaScript.</param>
        /// <returns>True if the two BsonJavaScript values are equal according to ==.</returns>
        public static bool operator ==(BsonJavaScript lhs, BsonJavaScript rhs)
        {
            if (object.ReferenceEquals(lhs, null)) { return object.ReferenceEquals(rhs, null); }
            return lhs.Equals(rhs);
        }

        // public operators
        /// <summary>
        /// Converts a string to a BsonJavaScript.
        /// </summary>
        /// <param name="code">A string.</param>
        /// <returns>A BsonJavaScript.</returns>
        public static implicit operator BsonJavaScript(string code)
        {
            return BsonJavaScript.Create(code);
        }

        /// <summary>
        /// Creates a new instance of the BsonJavaScript class.
        /// </summary>
        /// <param name="code">A string containing JavaScript code.</param>
        /// <returns>A BsonJavaScript.</returns>
        public static BsonJavaScript Create(string code)
        {
            if (code != null)
            {
                return new BsonJavaScript(code);
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Creates a new BsonJavaScript.
        /// </summary>
        /// <param name="value">An object to be mapped to a BsonJavaScript.</param>
        /// <returns>A BsonJavaScript or null.</returns>
        public new static BsonJavaScript Create(object value)
        {
            if (value != null)
            {
                return (BsonJavaScript)BsonTypeMapper.MapToBsonValue(value, BsonType.JavaScript);
            }
            else
            {
                return null;
            }
        }

        // public methods
        /// <summary>
        /// Compares this BsonJavaScript to another BsonJavaScript.
        /// </summary>
        /// <param name="other">The other BsonJavaScript.</param>
        /// <returns>A 32-bit signed integer that indicates whether this BsonJavaScript is less than, equal to, or greather than the other.</returns>
        public int CompareTo(BsonJavaScript other)
        {
            if (other == null) { return 1; }
            return _code.CompareTo(other._code);
        }

        /// <summary>
        /// Compares the BsonJavaScript to another BsonValue.
        /// </summary>
        /// <param name="other">The other BsonValue.</param>
        /// <returns>A 32-bit signed integer that indicates whether this BsonJavaScript is less than, equal to, or greather than the other BsonValue.</returns>
        public override int CompareTo(BsonValue other)
        {
            if (other == null) { return 1; }
            var otherJavaScript = other as BsonJavaScript;
            if (otherJavaScript != null)
            {
                return CompareTo(otherJavaScript);
            }
            return CompareTypeTo(other);
        }

        /// <summary>
        /// Compares this BsonJavaScript to another BsonJavaScript.
        /// </summary>
        /// <param name="rhs">The other BsonJavaScript.</param>
        /// <returns>True if the two BsonJavaScript values are equal.</returns>
        public bool Equals(BsonJavaScript rhs)
        {
            if (object.ReferenceEquals(rhs, null) || GetType() != rhs.GetType()) { return false; }
            return _code == rhs._code;
        }

        /// <summary>
        /// Compares this BsonJavaScript to another object.
        /// </summary>
        /// <param name="obj">The other object.</param>
        /// <returns>True if the other object is a BsonJavaScript and equal to this one.</returns>
        public override bool Equals(object obj)
        {
            return Equals(obj as BsonJavaScript); // works even if obj is null or of a different type
        }

        /// <summary>
        /// Gets the hash code.
        /// </summary>
        /// <returns>The hash code.</returns>
        public override int GetHashCode()
        {
            // see Effective Java by Joshua Bloch
            int hash = 17;
            hash = 37 * hash + BsonType.GetHashCode();
            hash = 37 * hash + _code.GetHashCode();
            return hash;
        }

        /// <summary>
        /// Returns a string representation of the value.
        /// </summary>
        /// <returns>A string representation of the value.</returns>
        public override string ToString()
        {
            return string.Format("new BsonJavaScript(\"{0}\")", _code);
        }
    }
}
