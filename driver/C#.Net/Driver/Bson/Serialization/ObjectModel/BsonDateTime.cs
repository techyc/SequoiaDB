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
using System.Xml;

namespace SqlDB.Bson
{
    /// <summary>
    /// Represents a BSON DateTime value.
    /// </summary>
    [Serializable]
    public class BsonDateTime : BsonValue, IComparable<BsonDateTime>, IEquatable<BsonDateTime>
    {
        // private fields
        private long _millisecondsSinceEpoch;

        // constructors
        /// <summary>
        /// Initializes a new instance of the BsonDateTime class.
        /// </summary>
        /// <param name="value">A DateTime.</param>
        public BsonDateTime(DateTime value)
            : base(BsonType.DateTime)
        {
            _millisecondsSinceEpoch = BsonUtils.ToMillisecondsSinceEpoch(value);
        }

        /// <summary>
        /// Initializes a new instance of the BsonDateTime class.
        /// </summary>
        /// <param name="millisecondsSinceEpoch">Milliseconds since Unix Epoch.</param>
        public BsonDateTime(long millisecondsSinceEpoch)
            : base(BsonType.DateTime)
        {
            _millisecondsSinceEpoch = millisecondsSinceEpoch;
        }

        // public properties
        /// <summary>
        /// Gets whether this BsonDateTime is a valid .NET DateTime.
        /// </summary>
        public bool IsValidDateTime
        {
            get
            {
                return
                    _millisecondsSinceEpoch >= BsonConstants.DateTimeMinValueMillisecondsSinceEpoch &&
                    _millisecondsSinceEpoch <= BsonConstants.DateTimeMaxValueMillisecondsSinceEpoch;
            }
        }

        /// <summary>
        /// Gets the number of milliseconds since the Unix Epoch.
        /// </summary>
        public long MillisecondsSinceEpoch
        {
            get { return _millisecondsSinceEpoch; }
        }

        /// <summary>
        /// Gets the number of milliseconds since the Unix Epoch.
        /// </summary>
        public override object RawValue
        {
            get { return _millisecondsSinceEpoch; }
        }

        /// <summary>
        /// Gets the DateTime value.
        /// </summary>
        public DateTime Value
        {
            get
            {
                return BsonUtils.ToDateTimeFromMillisecondsSinceEpoch(_millisecondsSinceEpoch);
            }
        }

        // public operators
        /// <summary>
        /// Converts a DateTime to a BsonDateTime.
        /// </summary>
        /// <param name="value">A DateTime.</param>
        /// <returns>A BsonDateTime.</returns>
        public static implicit operator BsonDateTime(DateTime value)
        {
            return new BsonDateTime(value);
        }

        /// <summary>
        /// Compares two BsonDateTime values.
        /// </summary>
        /// <param name="lhs">The first BsonDateTime.</param>
        /// <param name="rhs">The other BsonDateTime.</param>
        /// <returns>True if the two BsonDateTime values are not equal according to ==.</returns>
        public static bool operator !=(BsonDateTime lhs, BsonDateTime rhs)
        {
            return !(lhs == rhs);
        }

        /// <summary>
        /// Compares two BsonDateTime values.
        /// </summary>
        /// <param name="lhs">The first BsonDateTime.</param>
        /// <param name="rhs">The other BsonDateTime.</param>
        /// <returns>True if the two BsonDateTime values are equal according to ==.</returns>
        public static bool operator ==(BsonDateTime lhs, BsonDateTime rhs)
        {
            if (object.ReferenceEquals(lhs, null)) { return object.ReferenceEquals(rhs, null); }
            return lhs.Equals(rhs);
        }

        // public static methods
        /// <summary>
        /// Creates a new BsonDateTime.
        /// </summary>
        /// <param name="value">A DateTime.</param>
        /// <returns>A BsonDateTime.</returns>
        public static BsonDateTime Create(DateTime value)
        {
            return new BsonDateTime(value);
        }

        /// <summary>
        /// Creates a new BsonDateTime.
        /// </summary>
        /// <param name="millisecondsSinceEpoch">A DateTime.</param>
        /// <returns>Milliseconds since Unix Epoch.</returns>
        public static BsonDateTime Create(long millisecondsSinceEpoch)
        {
            return new BsonDateTime(millisecondsSinceEpoch);
        }

        /// <summary>
        /// Creates a new BsonDateTime.
        /// </summary>
        /// <param name="value">An object to be mapped to a BsonDateTime.</param>
        /// <returns>A BsonDateTime or null.</returns>
        public new static BsonDateTime Create(object value)
        {
            if (value != null)
            {
                return (BsonDateTime)BsonTypeMapper.MapToBsonValue(value, BsonType.DateTime);
            }
            else
            {
                return null;
            }
        }

        // public methods
        /// <summary>
        /// Compares this BsonDateTime to another BsonDateTime.
        /// </summary>
        /// <param name="other">The other BsonDateTime.</param>
        /// <returns>A 32-bit signed integer that indicates whether this BsonDateTime is less than, equal to, or greather than the other.</returns>
        public int CompareTo(BsonDateTime other)
        {
            if (other == null) { return 1; }
            return _millisecondsSinceEpoch.CompareTo(other._millisecondsSinceEpoch);
        }

        /// <summary>
        /// Compares the BsonDateTime to another BsonValue.
        /// </summary>
        /// <param name="other">The other BsonValue.</param>
        /// <returns>A 32-bit signed integer that indicates whether this BsonDateTime is less than, equal to, or greather than the other BsonValue.</returns>
        public override int CompareTo(BsonValue other)
        {
            if (other == null) { return 1; }
            var otherDateTime = other as BsonDateTime;
            if (otherDateTime != null)
            {
                return _millisecondsSinceEpoch.CompareTo(otherDateTime._millisecondsSinceEpoch);
            }
            var otherTimestamp = other as BsonTimestamp;
            if (otherTimestamp != null)
            {
                return _millisecondsSinceEpoch.CompareTo(otherTimestamp.Timestamp * 1000L); // timestamp is in seconds
            }
            return CompareTypeTo(other);
        }

        /// <summary>
        /// Compares this BsonDateTime to another BsonDateTime.
        /// </summary>
        /// <param name="rhs">The other BsonDateTime.</param>
        /// <returns>True if the two BsonDateTime values are equal.</returns>
        public bool Equals(BsonDateTime rhs)
        {
            if (object.ReferenceEquals(rhs, null) || GetType() != rhs.GetType()) { return false; }
            return _millisecondsSinceEpoch == rhs._millisecondsSinceEpoch;
        }

        /// <summary>
        /// Compares this BsonDateTime to another object.
        /// </summary>
        /// <param name="obj">The other object.</param>
        /// <returns>True if the other object is a BsonDateTime and equal to this one.</returns>
        public override bool Equals(object obj)
        {
            return Equals(obj as BsonDateTime); // works even if obj is null or of a different type
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
            hash = 37 * hash + _millisecondsSinceEpoch.GetHashCode();
            return hash;
        }

        /// <summary>
        /// Converts the BsonDateTime value to a .NET DateTime value in the local timezone.
        /// </summary>
        /// <returns>A DateTime in the local timezone.</returns>
        public DateTime ToLocalTime()
        {
            var utcDateTime = BsonUtils.ToDateTimeFromMillisecondsSinceEpoch(_millisecondsSinceEpoch);
            return BsonUtils.ToLocalTime(utcDateTime);
        }

        /// <summary>
        /// Converts the BsonDateTime value to a .NET DateTime value in UTC.
        /// </summary>
        /// <returns>A DateTime in UTC.</returns>
        public DateTime ToUniversalTime()
        {
            return BsonUtils.ToDateTimeFromMillisecondsSinceEpoch(_millisecondsSinceEpoch);
        }

        /// <summary>
        /// Returns a string representation of the value.
        /// </summary>
        /// <returns>A string representation of the value.</returns>
        public override string ToString()
        {
            if (IsValidDateTime)
            {
                return BsonUtils.ToDateTimeFromMillisecondsSinceEpoch(_millisecondsSinceEpoch).ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFFK");
            }
            else
            {
                return XmlConvert.ToString(_millisecondsSinceEpoch);
            }
        }
    }
}
