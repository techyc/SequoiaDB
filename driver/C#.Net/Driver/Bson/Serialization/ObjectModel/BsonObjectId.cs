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
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace SqlDB.Bson
{
    /// <summary>
    /// Represents a BSON ObjectId value (see also ObjectId).
    /// </summary>
    [Serializable]
    public class BsonObjectId : BsonValue, IComparable<BsonObjectId>, IEquatable<BsonObjectId>
    {
        // private static fields
        private static BsonObjectId __emptyInstance = new BsonObjectId(ObjectId.Empty);

        // private fields
        private ObjectId _value;

        // constructors
        /// <summary>
        /// Initializes a new instance of the BsonObjectId class.
        /// </summary>
        /// <param name="value">The value.</param>
        public BsonObjectId(ObjectId value)
            : base(BsonType.ObjectId)
        {
            _value = value;
        }

        /// <summary>
        /// Initializes a new instance of the BsonObjectId class.
        /// </summary>
        /// <param name="bytes">The bytes.</param>
        public BsonObjectId(byte[] bytes)
            : base(BsonType.ObjectId)
        {
            _value = new ObjectId(bytes);
        }

        /// <summary>
        /// Initializes a new instance of the BsonObjectId class.
        /// </summary>
        /// <param name="timestamp">The timestamp (expressed as a DateTime).</param>
        /// <param name="machine">The machine hash.</param>
        /// <param name="pid">The PID.</param>
        /// <param name="increment">The increment.</param>
        public BsonObjectId(DateTime timestamp, int machine, short pid, int increment)
            : base(BsonType.ObjectId)
        {
            _value = new ObjectId(timestamp, machine, pid, increment);
        }

        /// <summary>
        /// Initializes a new instance of the BsonObjectId class.
        /// </summary>
        /// <param name="timestamp">The timestamp.</param>
        /// <param name="machine">The machine hash.</param>
        /// <param name="pid">The PID.</param>
        /// <param name="increment">The increment.</param>
        public BsonObjectId(int timestamp, int machine, short pid, int increment)
            : base(BsonType.ObjectId)
        {
            _value = new ObjectId(timestamp, machine, pid, increment);
        }

        /// <summary>
        /// Initializes a new instance of the BsonObjectId class.
        /// </summary>
        /// <param name="value">The value.</param>
        public BsonObjectId(string value)
            : base(BsonType.ObjectId)
        {
            _value = new ObjectId(value);
        }

        // public static properties
        /// <summary>
        /// Gets an instance of BsonObjectId where the value is empty.
        /// </summary>
        public static BsonObjectId Empty
        {
            get { return __emptyInstance; }
        }

        // public properties
        /// <summary>
        /// Gets the timestamp.
        /// </summary>
        public int Timestamp
        {
            get { return _value.Timestamp; }
        }

        /// <summary>
        /// Gets the machine.
        /// </summary>
        public int Machine
        {
            get { return _value.Machine; }
        }

        /// <summary>
        /// Gets the PID.
        /// </summary>
        public short Pid
        {
            get { return _value.Pid; }
        }

        /// <summary>
        /// Gets the increment.
        /// </summary>
        public int Increment
        {
            get { return _value.Increment; }
        }

        /// <summary>
        /// Gets the creation time (derived from the timestamp).
        /// </summary>
        public DateTime CreationTime
        {
            get { return _value.CreationTime; }
        }

        /// <summary>
        /// Gets the BsonObjectId as an ObjectId.
        /// </summary>
        public override object RawValue
        {
            get { return _value; }
        }

        /// <summary>
        /// Gets the value of this BsonObjectId.
        /// </summary>
        public ObjectId Value
        {
            get { return _value; }
        }

        // public operators
        /// <summary>
        /// Converts an ObjectId to a BsonObjectId.
        /// </summary>
        /// <param name="value">An ObjectId.</param>
        /// <returns>A BsonObjectId.</returns>
        public static implicit operator BsonObjectId(ObjectId value)
        {
            return new BsonObjectId(value);
        }

        /// <summary>
        /// Compares two BsonObjectId values.
        /// </summary>
        /// <param name="lhs">The first BsonObjectId.</param>
        /// <param name="rhs">The other BsonObjectId.</param>
        /// <returns>True if the two BsonObjectId values are not equal according to ==.</returns>
        public static bool operator !=(BsonObjectId lhs, BsonObjectId rhs)
        {
            return !(lhs == rhs);
        }

        /// <summary>
        /// Compares two BsonObjectId values.
        /// </summary>
        /// <param name="lhs">The first BsonObjectId.</param>
        /// <param name="rhs">The other BsonObjectId.</param>
        /// <returns>True if the two BsonObjectId values are equal according to ==.</returns>
        public static bool operator ==(BsonObjectId lhs, BsonObjectId rhs)
        {
            if (object.ReferenceEquals(lhs, null)) { return object.ReferenceEquals(rhs, null); }
            return lhs.Equals(rhs);
        }

        // public static methods
        /// <summary>
        /// Creates a new instance of the BsonObjectId class.
        /// </summary>
        /// <param name="value">An ObjectId.</param>
        /// <returns>A BsonObjectId.</returns>
        public static BsonObjectId Create(ObjectId value)
        {
            return new BsonObjectId(value);
        }

        /// <summary>
        /// Creates a new instance of the BsonObjectId class.
        /// </summary>
        /// <param name="value">A byte array.</param>
        /// <returns>A BsonObjectId.</returns>
        public static BsonObjectId Create(byte[] value)
        {
            if (value != null)
            {
                return new BsonObjectId(value);
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Creates a new instance of the BsonObjectId class.
        /// </summary>
        /// <param name="timestamp">The timestamp.</param>
        /// <param name="machine">The machine hash.</param>
        /// <param name="pid">The pid.</param>
        /// <param name="increment">The increment.</param>
        /// <returns>A BsonObjectId.</returns>
        public static BsonObjectId Create(int timestamp, int machine, short pid, int increment)
        {
            return new BsonObjectId(timestamp, machine, pid, increment);
        }

        /// <summary>
        /// Creates a new BsonObjectId.
        /// </summary>
        /// <param name="value">An object to be mapped to a BsonObjectId.</param>
        /// <returns>A BsonObjectId or null.</returns>
        public new static BsonObjectId Create(object value)
        {
            if (value != null)
            {
                return (BsonObjectId)BsonTypeMapper.MapToBsonValue(value, BsonType.ObjectId);
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Creates a new instance of the BsonObjectId class.
        /// </summary>
        /// <param name="value">A string.</param>
        /// <returns>A BsonObjectId.</returns>
        public static BsonObjectId Create(string value)
        {
            if (value != null)
            {
                return new BsonObjectId(value);
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Generates a new BsonObjectId with a unique value.
        /// </summary>
        /// <returns>A BsonObjectId.</returns>
        public static BsonObjectId GenerateNewId()
        {
            return new BsonObjectId(ObjectId.GenerateNewId());
        }

        /// <summary>
        /// Generates a new BsonObjectId with a unique value (with the timestamp component based on a given DateTime).
        /// </summary>
        /// <param name="timestamp">The timestamp component (expressed as a DateTime).</param>
        /// <returns>A BsonObjectId.</returns>
        public static BsonObjectId GenerateNewId(DateTime timestamp)
        {
            return new BsonObjectId(ObjectId.GenerateNewId(timestamp));
        }

        /// <summary>
        /// Generates a new BsonObjectId with a unique value (with the given timestamp).
        /// </summary>
        /// <param name="timestamp">The timestamp component.</param>
        /// <returns>A BsonObjectId.</returns>
        public static BsonObjectId GenerateNewId(int timestamp)
        {
            return new BsonObjectId(ObjectId.GenerateNewId(timestamp));
        }

        /// <summary>
        /// Parses a string and creates a new BsonObjectId.
        /// </summary>
        /// <param name="s">The string value.</param>
        /// <returns>A BsonObjectId.</returns>
        public static BsonObjectId Parse(string s)
        {
            return new BsonObjectId(ObjectId.Parse(s));
        }

        /// <summary>
        /// Tries to parse a string and create a new BsonObjectId.
        /// </summary>
        /// <param name="s">The string value.</param>
        /// <param name="value">The new BsonObjectId.</param>
        /// <returns>True if the string was parsed successfully.</returns>
        public static bool TryParse(string s, out BsonObjectId value)
        {
            // don't throw ArgumentNullException if s is null
            ObjectId objectId;
            if (ObjectId.TryParse(s, out objectId))
            {
                value = new BsonObjectId(objectId);
                return true;
            }
            else
            {
                value = null;
                return false;
            }
        }

        // public methods
        /// <summary>
        /// Compares this BsonObjectId to another BsonObjectId.
        /// </summary>
        /// <param name="other">The other BsonObjectId.</param>
        /// <returns>A 32-bit signed integer that indicates whether this BsonObjectId is less than, equal to, or greather than the other.</returns>
        public int CompareTo(BsonObjectId other)
        {
            if (other == null) { return 1; }
            return _value.CompareTo(other.Value);
        }

        /// <summary>
        /// Compares the BsonObjectId to another BsonValue.
        /// </summary>
        /// <param name="other">The other BsonValue.</param>
        /// <returns>A 32-bit signed integer that indicates whether this BsonObjectId is less than, equal to, or greather than the other BsonValue.</returns>
        public override int CompareTo(BsonValue other)
        {
            if (other == null) { return 1; }
            var otherObjectId = other as BsonObjectId;
            if (otherObjectId != null)
            {
                return _value.CompareTo(otherObjectId.Value);
            }
            return CompareTypeTo(other);
        }

        /// <summary>
        /// Compares this BsonObjectId to another BsonObjectId.
        /// </summary>
        /// <param name="rhs">The other BsonObjectId.</param>
        /// <returns>True if the two BsonObjectId values are equal.</returns>
        public bool Equals(BsonObjectId rhs)
        {
            if (object.ReferenceEquals(rhs, null) || GetType() != rhs.GetType()) { return false; }
            return this.Value == rhs.Value;
        }

        /// <summary>
        /// Compares this BsonObjectId to another object.
        /// </summary>
        /// <param name="obj">The other object.</param>
        /// <returns>True if the other object is a BsonObjectId and equal to this one.</returns>
        public override bool Equals(object obj)
        {
            return Equals(obj as BsonObjectId); // works even if obj is null or of a different type
        }

        /// <summary>
        /// Gets the hash code.
        /// </summary>
        /// <returns>The hash code.</returns>
        public override int GetHashCode()
        {
            int hash = 17;
            hash = 37 * hash + BsonType.GetHashCode();
            hash = 37 * hash + _value.GetHashCode();
            return hash;
        }

        /// <summary>
        /// Converts the BsonObjectId to a byte array.
        /// </summary>
        /// <returns>A byte array.</returns>
        public byte[] ToByteArray()
        {
            return _value.ToByteArray();
        }

        /// <summary>
        /// Returns a string representation of the value.
        /// </summary>
        /// <returns>A string representation of the value.</returns>
        public override string ToString()
        {
            return _value.ToString();
        }
    }
}
