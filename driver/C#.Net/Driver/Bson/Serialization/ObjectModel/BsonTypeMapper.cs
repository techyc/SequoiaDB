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
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;

namespace SqlDB.Bson
{
    /// <summary>
    /// A static class that maps between .NET objects and BsonValues.
    /// </summary>
    public static class BsonTypeMapper
    {
        // private static fields
        // table of from mappings used by MapToBsonValue
        private static Dictionary<Type, Conversion> __fromMappings = new Dictionary<Type, Conversion>
        {
            { typeof(bool), Conversion.NewBsonBoolean },
            { typeof(byte), Conversion.ByteToBsonInt32 },
            { typeof(byte[]), Conversion.ByteArrayToBsonBinary },
            { typeof(DateTime), Conversion.DateTimeToBsonDateTime },
            { typeof(double), Conversion.NewBsonDouble },
            { typeof(float), Conversion.SingleToBsonDouble },
            { typeof(Guid), Conversion.GuidToBsonBinary },
            { typeof(int), Conversion.NewBsonInt32 },
            { typeof(long), Conversion.NewBsonInt64 },
            { typeof(ObjectId), Conversion.NewBsonObjectId },
            { typeof(Regex), Conversion.RegexToBsonRegularExpression },
            { typeof(sbyte), Conversion.SByteToBsonInt32 },
            { typeof(short), Conversion.Int16ToBsonInt32 },
            { typeof(string), Conversion.NewBsonString },
            { typeof(uint), Conversion.UInt32ToBsonInt64 },
            { typeof(ushort), Conversion.UInt16ToBsonInt32 },
            { typeof(ulong), Conversion.UInt64ToBsonInt64 }
        };

        // table of from/to mappings used by MapToBsonValue
        private static Dictionary<Mapping, Conversion> __fromToMappings = new Dictionary<Mapping, Conversion>()
        {
            { Mapping.FromTo(typeof(bool), BsonType.Boolean), Conversion.NewBsonBoolean },
            { Mapping.FromTo(typeof(BsonArray), BsonType.Array), Conversion.None },
            { Mapping.FromTo(typeof(BsonBinaryData), BsonType.Binary), Conversion.None },
            { Mapping.FromTo(typeof(BsonBoolean), BsonType.Boolean), Conversion.None },
            { Mapping.FromTo(typeof(BsonDateTime), BsonType.DateTime), Conversion.None },
            { Mapping.FromTo(typeof(BsonDocument), BsonType.Document), Conversion.None },
            { Mapping.FromTo(typeof(BsonDouble), BsonType.Double), Conversion.None },
            { Mapping.FromTo(typeof(BsonInt32), BsonType.Int32), Conversion.None },
            { Mapping.FromTo(typeof(BsonInt64), BsonType.Int64), Conversion.None },
            { Mapping.FromTo(typeof(BsonJavaScript), BsonType.JavaScript), Conversion.None },
            { Mapping.FromTo(typeof(BsonJavaScript), BsonType.JavaScriptWithScope), Conversion.BsonJavaScriptToBsonJavaScriptWithScope },
            { Mapping.FromTo(typeof(BsonJavaScriptWithScope), BsonType.JavaScriptWithScope), Conversion.None },
            { Mapping.FromTo(typeof(BsonMaxKey), BsonType.Boolean), Conversion.BsonMaxKeyToBsonBoolean },
            { Mapping.FromTo(typeof(BsonMaxKey), BsonType.MaxKey), Conversion.None },
            { Mapping.FromTo(typeof(BsonMinKey), BsonType.Boolean), Conversion.BsonMinKeyToBsonBoolean },
            { Mapping.FromTo(typeof(BsonMinKey), BsonType.MinKey), Conversion.None },
            { Mapping.FromTo(typeof(BsonNull), BsonType.Boolean), Conversion.BsonNullToBsonBoolean },
            { Mapping.FromTo(typeof(BsonNull), BsonType.Null), Conversion.None },
            { Mapping.FromTo(typeof(BsonObjectId), BsonType.ObjectId), Conversion.None },
            { Mapping.FromTo(typeof(BsonRegularExpression), BsonType.RegularExpression), Conversion.None },
            { Mapping.FromTo(typeof(BsonString), BsonType.String), Conversion.None },
            { Mapping.FromTo(typeof(BsonSymbol), BsonType.Symbol), Conversion.None },
            { Mapping.FromTo(typeof(BsonTimestamp), BsonType.Timestamp), Conversion.None },
            { Mapping.FromTo(typeof(BsonUndefined), BsonType.Boolean), Conversion.BsonUndefinedToBsonBoolean },
            { Mapping.FromTo(typeof(BsonUndefined), BsonType.Undefined), Conversion.None },
            { Mapping.FromTo(typeof(byte), BsonType.Boolean), Conversion.ByteToBsonBoolean },
            { Mapping.FromTo(typeof(byte), BsonType.Double), Conversion.ByteToBsonDouble },
            { Mapping.FromTo(typeof(byte), BsonType.Int32), Conversion.ByteToBsonInt32 },
            { Mapping.FromTo(typeof(byte), BsonType.Int64), Conversion.ByteToBsonInt64 },
            { Mapping.FromTo(typeof(byte[]), BsonType.Binary), Conversion.ByteArrayToBsonBinary },
            { Mapping.FromTo(typeof(byte[]), BsonType.ObjectId), Conversion.ByteArrayToBsonObjectId },
            { Mapping.FromTo(typeof(DateTime), BsonType.DateTime), Conversion.DateTimeToBsonDateTime },
            { Mapping.FromTo(typeof(DateTimeOffset), BsonType.DateTime), Conversion.DateTimeOffsetToBsonDateTime },
            { Mapping.FromTo(typeof(double), BsonType.Boolean), Conversion.DoubleToBsonBoolean },
            { Mapping.FromTo(typeof(double), BsonType.Double), Conversion.NewBsonDouble },
            { Mapping.FromTo(typeof(float), BsonType.Boolean), Conversion.SingleToBsonBoolean },
            { Mapping.FromTo(typeof(float), BsonType.Double), Conversion.SingleToBsonDouble },
            { Mapping.FromTo(typeof(Guid), BsonType.Binary), Conversion.GuidToBsonBinary },
            { Mapping.FromTo(typeof(int), BsonType.Boolean), Conversion.Int32ToBsonBoolean },
            { Mapping.FromTo(typeof(int), BsonType.Double), Conversion.Int32ToBsonDouble },
            { Mapping.FromTo(typeof(int), BsonType.Int32), Conversion.NewBsonInt32 },
            { Mapping.FromTo(typeof(int), BsonType.Int64), Conversion.Int32ToBsonInt64 },
            { Mapping.FromTo(typeof(long), BsonType.Boolean), Conversion.Int64ToBsonBoolean },
            { Mapping.FromTo(typeof(long), BsonType.Double), Conversion.Int64ToBsonDouble },
            { Mapping.FromTo(typeof(long), BsonType.Int64), Conversion.NewBsonInt64 },
            { Mapping.FromTo(typeof(long), BsonType.Timestamp), Conversion.Int64ToBsonTimestamp },
            { Mapping.FromTo(typeof(ObjectId), BsonType.ObjectId), Conversion.NewBsonObjectId },
            { Mapping.FromTo(typeof(Regex), BsonType.RegularExpression), Conversion.RegexToBsonRegularExpression },
            { Mapping.FromTo(typeof(sbyte), BsonType.Boolean), Conversion.SByteToBsonBoolean },
            { Mapping.FromTo(typeof(sbyte), BsonType.Double), Conversion.SByteToBsonDouble },
            { Mapping.FromTo(typeof(sbyte), BsonType.Int32), Conversion.SByteToBsonInt32 },
            { Mapping.FromTo(typeof(sbyte), BsonType.Int64), Conversion.SByteToBsonInt64 },
            { Mapping.FromTo(typeof(short), BsonType.Boolean), Conversion.Int16ToBsonBoolean },
            { Mapping.FromTo(typeof(short), BsonType.Double), Conversion.Int16ToBsonDouble },
            { Mapping.FromTo(typeof(short), BsonType.Int32), Conversion.Int16ToBsonInt32 },
            { Mapping.FromTo(typeof(short), BsonType.Int64), Conversion.Int16ToBsonInt64 },
            { Mapping.FromTo(typeof(string), BsonType.Boolean), Conversion.StringToBsonBoolean },
            { Mapping.FromTo(typeof(string), BsonType.DateTime), Conversion.StringToBsonDateTime },
            { Mapping.FromTo(typeof(string), BsonType.Double), Conversion.StringToBsonDouble },
            { Mapping.FromTo(typeof(string), BsonType.Int32), Conversion.StringToBsonInt32 },
            { Mapping.FromTo(typeof(string), BsonType.Int64), Conversion.StringToBsonInt64 },
            { Mapping.FromTo(typeof(string), BsonType.JavaScript), Conversion.StringToBsonJavaScript },
            { Mapping.FromTo(typeof(string), BsonType.JavaScriptWithScope), Conversion.StringToBsonJavaScriptWithScope },
            { Mapping.FromTo(typeof(string), BsonType.ObjectId), Conversion.StringToBsonObjectId },
            { Mapping.FromTo(typeof(string), BsonType.RegularExpression), Conversion.StringToBsonRegularExpression },
            { Mapping.FromTo(typeof(string), BsonType.String), Conversion.NewBsonString },
            { Mapping.FromTo(typeof(string), BsonType.Symbol), Conversion.StringToBsonSymbol },
            { Mapping.FromTo(typeof(uint), BsonType.Boolean), Conversion.UInt32ToBsonBoolean },
            { Mapping.FromTo(typeof(uint), BsonType.Double), Conversion.UInt32ToBsonDouble },
            { Mapping.FromTo(typeof(uint), BsonType.Int32), Conversion.UInt32ToBsonInt32 },
            { Mapping.FromTo(typeof(uint), BsonType.Int64), Conversion.UInt32ToBsonInt64 },
            { Mapping.FromTo(typeof(ushort), BsonType.Boolean), Conversion.UInt16ToBsonBoolean },
            { Mapping.FromTo(typeof(ushort), BsonType.Double), Conversion.UInt16ToBsonDouble },
            { Mapping.FromTo(typeof(ushort), BsonType.Int32), Conversion.UInt16ToBsonInt32 },
            { Mapping.FromTo(typeof(ushort), BsonType.Int64), Conversion.UInt16ToBsonInt64 },
            { Mapping.FromTo(typeof(ulong), BsonType.Boolean), Conversion.UInt64ToBsonBoolean },
            { Mapping.FromTo(typeof(ulong), BsonType.Double), Conversion.UInt64ToBsonDouble },
            { Mapping.FromTo(typeof(ulong), BsonType.Int64), Conversion.UInt64ToBsonInt64 },
            { Mapping.FromTo(typeof(ulong), BsonType.Timestamp), Conversion.UInt64ToBsonTimestamp }
        };

        private static Dictionary<Type, ICustomBsonTypeMapper> __customTypeMappers = new Dictionary<Type, ICustomBsonTypeMapper>();

        // public static methods
        /// <summary>
        /// Maps an object to an instance of the closest BsonValue class.
        /// </summary>
        /// <param name="value">An object.</param>
        /// <returns>A BsonValue.</returns>
        public static BsonValue MapToBsonValue(object value)
        {
            BsonValue bsonValue;
            if (TryMapToBsonValue(value, out bsonValue))
            {
                return bsonValue;
            }

            var message = string.Format(".NET type {0} cannot be mapped to a BsonValue.", value.GetType().FullName);
            throw new ArgumentException(message);
        }

        /// <summary>
        /// Maps an object to a specific BsonValue type.
        /// </summary>
        /// <param name="value">An object.</param>
        /// <param name="bsonType">The BsonType to map to.</param>
        /// <returns>A BsonValue of the desired type (or BsonNull.Value if value is null and bsonType is Null).</returns>
        public static BsonValue MapToBsonValue(object value, BsonType bsonType)
        {
            string message;
            if (value == null)
            {
                if (bsonType == BsonType.Null)
                {
                    return BsonNull.Value;
                }
                else
                {
                    message = string.Format("C# null cannot be mapped to BsonType.{0}.", bsonType);
                    throw new ArgumentException(message, "value");
                }
            }

            // handle subclasses of BsonDocument (like QueryDocument) correctly
            if (bsonType == BsonType.Document)
            {
                var bsonDocument = value as BsonDocument;
                if (bsonDocument != null)
                {
                    return bsonDocument;
                }
            }

            var valueType = value.GetType();
            if (valueType.IsEnum)
            {
                valueType = Enum.GetUnderlyingType(valueType);
                switch (Type.GetTypeCode(valueType))
                {
                    case TypeCode.Byte: value = (int)(byte)value; break;
                    case TypeCode.Int16: value = (int)(short)value; break;
                    case TypeCode.Int32: value = (int)value; break;
                    case TypeCode.Int64: value = (long)value; break;
                    case TypeCode.SByte: value = (int)(sbyte)value; break;
                    case TypeCode.UInt16: value = (int)(ushort)value; break;
                    case TypeCode.UInt32: value = (long)(uint)value; break;
                    case TypeCode.UInt64: value = (long)(ulong)value; break;
                }
                valueType = value.GetType();
            }

            Conversion conversion; // the conversion (if it exists) that will convert value to bsonType
            if (__fromToMappings.TryGetValue(Mapping.FromTo(valueType, bsonType), out conversion))
            {
                return Convert(value, conversion);
            }

            // these coercions can't be handled by the conversions table (because of the interfaces)
            switch (bsonType)
            {
                case BsonType.Array:
                    if (value is IEnumerable)
                    {
                        return new BsonArray((IEnumerable)value);
                    }
                    break;
                case BsonType.Document:
                    if (value is IDictionary<string, object>)
                    {
                        return new BsonDocument((IDictionary<string, object>)value);
                    }
                    if (value is IDictionary)
                    {
                        return new BsonDocument((IDictionary)value);
                    }
                    break;
            }

            message = string.Format(".NET type {0} cannot be mapped to BsonType.{1}.", value.GetType().FullName, bsonType);
            throw new ArgumentException(message, "value");
        }

        /// <summary>
        /// Maps a BsonValue to a .NET value using the default BsonTypeMapperOptions.
        /// </summary>
        /// <param name="bsonValue">The BsonValue.</param>
        /// <returns>The mapped .NET value.</returns>
        public static object MapToDotNetValue(BsonValue bsonValue)
        {
            return MapToDotNetValue(bsonValue, BsonTypeMapperOptions.Defaults);
        }

        /// <summary>
        /// Maps a BsonValue to a .NET value.
        /// </summary>
        /// <param name="bsonValue">The BsonValue.</param>
        /// <param name="options">The BsonTypeMapperOptions.</param>
        /// <returns>The mapped .NET value.</returns>
        public static object MapToDotNetValue(BsonValue bsonValue, BsonTypeMapperOptions options)
        {
            switch (bsonValue.BsonType)
            {
                case BsonType.Array:
                    var bsonArray = (BsonArray)bsonValue;
                    if (options.MapBsonArrayTo == typeof(BsonArray))
                    {
                        return bsonArray;
                    }
                    else if (options.MapBsonArrayTo == typeof(object[]))
                    {
                        var array = new object[bsonArray.Count];
                        for (int i = 0; i < bsonArray.Count; i++)
                        {
                            array[i] = MapToDotNetValue(bsonArray[i], options);
                        }
                        return array;
                    }
                    else if (typeof(IList<object>).IsAssignableFrom(options.MapBsonArrayTo))
                    {
                        var list = (IList<object>)Activator.CreateInstance(options.MapBsonArrayTo);
                        for (int i = 0; i < bsonArray.Count; i++)
                        {
                            list.Add(MapToDotNetValue(bsonArray[i], options));
                        }
                        return list;
                    }
                    else if (typeof(IList).IsAssignableFrom(options.MapBsonArrayTo))
                    {
                        var list = (IList)Activator.CreateInstance(options.MapBsonArrayTo);
                        for (int i = 0; i < bsonArray.Count; i++)
                        {
                            list.Add(MapToDotNetValue(bsonArray[i], options));
                        }
                        return list;
                    }
                    else
                    {
                        var message = string.Format("A BsonArray can't be mapped to a {0}.", BsonUtils.GetFriendlyTypeName(options.MapBsonArrayTo));
                        throw new NotSupportedException(message);
                    }
                case BsonType.Binary:
#pragma warning disable 618 // about obsolete BsonBinarySubType.OldBinary
                    var bsonBinaryData = (BsonBinaryData)bsonValue;
                    if (bsonBinaryData.SubType == BsonBinarySubType.Binary ||
                        bsonBinaryData.SubType == BsonBinarySubType.OldBinary && options.MapOldBinaryToByteArray)
                    {
                        return bsonBinaryData.Bytes;
                    }
                    else if (bsonBinaryData.SubType == BsonBinarySubType.UuidLegacy || bsonBinaryData.SubType == BsonBinarySubType.UuidStandard)
                    {
                        return bsonBinaryData.ToGuid();
                    }
                    else
                    {
                        return bsonBinaryData; // unmapped
                    }
#pragma warning restore 618
                case BsonType.Boolean:
                    return bsonValue.AsBoolean;
                case BsonType.DateTime:
                    return bsonValue.AsUniversalTime;
                case BsonType.Document:
                    var bsonDocument = (BsonDocument)bsonValue;
                    if (options.MapBsonDocumentTo == typeof(BsonDocument))
                    {
                        return bsonDocument;
                    }
                    else if (typeof(IDictionary<string, object>).IsAssignableFrom(options.MapBsonDocumentTo))
                    {
                        var dictionary = (IDictionary<string, object>)Activator.CreateInstance(options.MapBsonDocumentTo);
                        foreach (var element in bsonDocument.Elements)
                        {
                            var mappedValue = MapToDotNetValue(element.Value, options);
                            if (dictionary.ContainsKey(element.Name))
                            {
                                switch (options.DuplicateNameHandling)
                                {
                                    case DuplicateNameHandling.Ignore:
                                        break;
                                    case DuplicateNameHandling.Overwrite:
                                    default:
                                        dictionary[element.Name] = mappedValue;
                                        break;
                                    case DuplicateNameHandling.ThrowException:
                                        var message = string.Format("Duplicate element name '{0}'.", element.Name);
                                        throw new ArgumentOutOfRangeException(message);
                                }
                            }
                            else
                            {
                                dictionary.Add(element.Name, mappedValue);
                            }
                        }
                        return dictionary;
                    }
                    else if (typeof(IDictionary).IsAssignableFrom(options.MapBsonDocumentTo))
                    {
                        var dictionary = (IDictionary)Activator.CreateInstance(options.MapBsonDocumentTo);
                        foreach (var element in bsonDocument.Elements)
                        {
                            var mappedValue = MapToDotNetValue(element.Value, options);
                            if (dictionary.Contains(element.Name))
                            {
                                switch (options.DuplicateNameHandling)
                                {
                                    case DuplicateNameHandling.Ignore:
                                        break;
                                    case DuplicateNameHandling.Overwrite:
                                    default:
                                        dictionary[element.Name] = mappedValue;
                                        break;
                                    case DuplicateNameHandling.ThrowException:
                                        var message = string.Format("Duplicate element name '{0}'.", element.Name);
                                        throw new ArgumentOutOfRangeException(message);
                                }
                            }
                            else
                            {
                                dictionary.Add(element.Name, mappedValue);
                            }
                        }
                        return dictionary;
                    }
                    else
                    {
                        var message = string.Format("A BsonDocument can't be mapped to a {0}.", BsonUtils.GetFriendlyTypeName(options.MapBsonArrayTo));
                        throw new NotSupportedException(message);
                    }
                case BsonType.Double:
                    return bsonValue.AsDouble;
                case BsonType.Int32:
                    return bsonValue.AsInt32;
                case BsonType.Int64:
                    return bsonValue.AsInt64;
                case BsonType.Null:
                    return null; // BsonValue.Null maps to C# null
                case BsonType.ObjectId:
                    return bsonValue.AsObjectId;
                case BsonType.String:
                    return bsonValue.AsString;

                case BsonType.JavaScript:
                case BsonType.JavaScriptWithScope:
                case BsonType.MaxKey:
                case BsonType.MinKey:
                case BsonType.RegularExpression:
                case BsonType.Symbol:
                case BsonType.Timestamp:
                case BsonType.Undefined:
                default:
                    return bsonValue; // unmapped
            }
        }

        /// <summary>
        /// Registers a custom type mapper.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="customTypeMapper">A custom type mapper.</param>
        public static void RegisterCustomTypeMapper(Type type, ICustomBsonTypeMapper customTypeMapper)
        {
            __customTypeMappers.Add(type, customTypeMapper);
        }

        /// <summary>
        /// Tries to map an object to an instance of the closest BsonValue class.
        /// </summary>
        /// <param name="value">An object.</param>
        /// <param name="bsonValue">The BsonValue.</param>
        /// <returns>True if the mapping was successfull.</returns>
        public static bool TryMapToBsonValue(object value, out BsonValue bsonValue)
        {
            if (value == null)
            {
                bsonValue = BsonNull.Value;
                return true;
            }

            // also handles subclasses of BsonDocument (like QueryDocument) correctly
            bsonValue = value as BsonValue;
            if (bsonValue != null)
            {
                return true;
            }

            var valueType = value.GetType();
            if (valueType.IsEnum)
            {
                valueType = Enum.GetUnderlyingType(valueType);
                switch (Type.GetTypeCode(valueType))
                {
                    case TypeCode.Byte: value = (int)(byte)value; break;
                    case TypeCode.Int16: value = (int)(short)value; break;
                    case TypeCode.Int32: value = (int)value; break;
                    case TypeCode.Int64: value = (long)value; break;
                    case TypeCode.SByte: value = (int)(sbyte)value; break;
                    case TypeCode.UInt16: value = (int)(ushort)value; break;
                    case TypeCode.UInt32: value = (long)(uint)value; break;
                    case TypeCode.UInt64: value = (long)(ulong)value; break;
                }
                valueType = value.GetType();
            }

            Conversion conversion;
            if (__fromMappings.TryGetValue(valueType, out conversion))
            {
                bsonValue = Convert(value, conversion);
                return true;
            }

            // these mappings can't be handled by the mappings table (because of the interfaces)
            if (value is IDictionary<string, object>)
            {
                bsonValue = new BsonDocument((IDictionary<string, object>)value);
                return true;
            }
            if (value is IDictionary)
            {
                bsonValue = new BsonDocument((IDictionary)value);
                return true;
            }

            // NOTE: the check for IEnumerable must be after the check for IDictionary
            // because IDictionary implements IEnumerable
            if (value is IEnumerable)
            {
                bsonValue = new BsonArray((IEnumerable)value);
                return true;
            }

            ICustomBsonTypeMapper customTypeMapper;
            if (__customTypeMappers.TryGetValue(valueType, out customTypeMapper))
            {
                return customTypeMapper.TryMapToBsonValue(value, out bsonValue);
            }

            bsonValue = null;
            return false;
        }

        // private static methods
        private static BsonValue Convert(object value, Conversion conversion)
        {
            // note: the ToBoolean conversions use the JavaScript definition of truthiness
            switch (conversion)
            {
                // note: I expect this switch statement to be compiled using a jump table and therefore to be very efficient
                case Conversion.None: return (BsonValue)value;
                case Conversion.BsonJavaScriptToBsonJavaScriptWithScope: return BsonJavaScriptWithScope.Create(((BsonJavaScript)value).Code, new BsonDocument());
                case Conversion.BsonMaxKeyToBsonBoolean: return BsonBoolean.True;
                case Conversion.BsonMinKeyToBsonBoolean: return BsonBoolean.True;
                case Conversion.BsonNullToBsonBoolean: return BsonBoolean.False;
                case Conversion.BsonUndefinedToBsonBoolean: return BsonBoolean.False;
                case Conversion.ByteArrayToBsonBinary: return new BsonBinaryData((byte[])value);
                case Conversion.ByteArrayToBsonObjectId: return BsonObjectId.Create((byte[])value);
                case Conversion.ByteToBsonBoolean: return BsonBoolean.Create((byte)value != 0);
                case Conversion.ByteToBsonDouble: return new BsonDouble((double)(byte)value);
                case Conversion.ByteToBsonInt32: return BsonInt32.Create((int)(byte)value);
                case Conversion.ByteToBsonInt64: return new BsonInt64((long)(byte)value);
                case Conversion.CharToBsonBoolean: return BsonBoolean.Create((char)value != 0);
                case Conversion.CharToBsonDouble: return new BsonDouble((double)(char)value);
                case Conversion.CharToBsonInt32: return BsonInt32.Create((int)(char)value);
                case Conversion.CharToBsonInt64: return new BsonInt64((long)(char)value);
                case Conversion.DateTimeOffsetToBsonDateTime: return new BsonDateTime(((DateTimeOffset)value).UtcDateTime);
                case Conversion.DateTimeToBsonDateTime: return new BsonDateTime((DateTime)value);
                case Conversion.DoubleToBsonBoolean: var d = (double)value; return BsonBoolean.Create(!(double.IsNaN(d) || d == 0.0));
                case Conversion.GuidToBsonBinary: return new BsonBinaryData((Guid)value);
                case Conversion.Int16ToBsonBoolean: return BsonBoolean.Create((short)value != 0);
                case Conversion.Int16ToBsonDouble: return new BsonDouble((double)(short)value);
                case Conversion.Int16ToBsonInt32: return BsonInt32.Create((int)(short)value);
                case Conversion.Int16ToBsonInt64: return new BsonInt64((long)(short)value);
                case Conversion.Int32ToBsonBoolean: return BsonBoolean.Create((int)value != 0);
                case Conversion.Int32ToBsonDouble: return new BsonDouble((double)(int)value);
                case Conversion.Int32ToBsonInt64: return new BsonInt64((long)(int)value);
                case Conversion.Int64ToBsonBoolean: return BsonBoolean.Create((long)value != 0);
                case Conversion.Int64ToBsonDouble: return new BsonDouble((double)(long)value);
                case Conversion.Int64ToBsonTimestamp: return new BsonTimestamp((long)value);
                case Conversion.NewBsonBoolean: return BsonBoolean.Create((bool)value);
                case Conversion.NewBsonDouble: return new BsonDouble((double)value);
                case Conversion.NewBsonInt32: return BsonInt32.Create((int)value);
                case Conversion.NewBsonInt64: return new BsonInt64((long)value);
                case Conversion.NewBsonObjectId: return new BsonObjectId((ObjectId)value);
                case Conversion.NewBsonString: return new BsonString((string)value);
                case Conversion.RegexToBsonRegularExpression: return new BsonRegularExpression((Regex)value);
                case Conversion.SByteToBsonBoolean: return BsonBoolean.Create((sbyte)value != 0);
                case Conversion.SByteToBsonDouble: return new BsonDouble((double)(sbyte)value);
                case Conversion.SByteToBsonInt32: return BsonInt32.Create((int)(sbyte)value);
                case Conversion.SByteToBsonInt64: return new BsonInt64((long)(sbyte)value);
                case Conversion.SingleToBsonBoolean: var f = (float)value; return BsonBoolean.Create(!(float.IsNaN(f) || f == 0.0f));
                case Conversion.SingleToBsonDouble: return new BsonDouble((double)(float)value);
                case Conversion.StringToBsonBoolean: return BsonBoolean.Create((string)value != "");
                case Conversion.StringToBsonDateTime:
                    var formats = new string[] { "yyyy-MM-ddK", "yyyy-MM-ddTHH:mm:ssK", "yyyy-MM-ddTHH:mm:ss.FFFFFFFK" };
                    var dt = DateTime.ParseExact((string)value, formats, null, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
                    return new BsonDateTime(dt);
                case Conversion.StringToBsonDouble: return new BsonDouble(XmlConvert.ToDouble((string)value));
                case Conversion.StringToBsonInt32: return BsonInt32.Create(XmlConvert.ToInt32((string)value));
                case Conversion.StringToBsonInt64: return new BsonInt64(XmlConvert.ToInt64((string)value));
                case Conversion.StringToBsonJavaScript: return new BsonJavaScript((string)value);
                case Conversion.StringToBsonJavaScriptWithScope: return new BsonJavaScriptWithScope((string)value, new BsonDocument());
                case Conversion.StringToBsonObjectId: return BsonObjectId.Create((string)value);
                case Conversion.StringToBsonRegularExpression: return new BsonRegularExpression((string)value);
                case Conversion.StringToBsonSymbol: return BsonSymbol.Create((string)value);
                case Conversion.StringToBsonTimestamp: return new BsonTimestamp(XmlConvert.ToInt64((string)value));
                case Conversion.UInt16ToBsonBoolean: return BsonBoolean.Create((ushort)value != 0);
                case Conversion.UInt16ToBsonDouble: return new BsonDouble((double)(ushort)value);
                case Conversion.UInt16ToBsonInt32: return BsonInt32.Create((int)(ushort)value);
                case Conversion.UInt16ToBsonInt64: return new BsonInt64((long)(ushort)value);
                case Conversion.UInt32ToBsonBoolean: return BsonBoolean.Create((uint)value != 0);
                case Conversion.UInt32ToBsonDouble: return new BsonDouble((double)(uint)value);
                case Conversion.UInt32ToBsonInt32: return BsonInt32.Create((int)(uint)value);
                case Conversion.UInt32ToBsonInt64: return new BsonInt64((long)(uint)value);
                case Conversion.UInt64ToBsonBoolean: return BsonBoolean.Create((ulong)value != 0);
                case Conversion.UInt64ToBsonDouble: return new BsonDouble((double)(ulong)value);
                case Conversion.UInt64ToBsonInt64: return new BsonInt64((long)(ulong)value);
                case Conversion.UInt64ToBsonTimestamp: return new BsonTimestamp((long)(ulong)value);
            }

            throw new BsonInternalException("Unexpected Conversion.");
        }

        // private nested types
        private enum Conversion
        {
            None,
            BsonJavaScriptToBsonJavaScriptWithScope,
            BsonMaxKeyToBsonBoolean,
            BsonMinKeyToBsonBoolean,
            BsonNullToBsonBoolean,
            BsonUndefinedToBsonBoolean,
            ByteArrayToBsonBinary,
            ByteArrayToBsonObjectId,
            ByteToBsonBoolean,
            ByteToBsonDouble,
            ByteToBsonInt32,
            ByteToBsonInt64,
            CharToBsonBoolean,
            CharToBsonDouble,
            CharToBsonInt32,
            CharToBsonInt64,
            DateTimeOffsetToBsonDateTime,
            DateTimeToBsonDateTime,
            DoubleToBsonBoolean,
            GuidToBsonBinary,
            Int16ToBsonBoolean,
            Int16ToBsonDouble,
            Int16ToBsonInt32,
            Int16ToBsonInt64,
            Int32ToBsonBoolean,
            Int32ToBsonDouble,
            Int32ToBsonInt64,
            Int64ToBsonBoolean,
            Int64ToBsonDouble,
            Int64ToBsonTimestamp,
            NewBsonBoolean,
            NewBsonDouble,
            NewBsonInt32,
            NewBsonInt64,
            NewBsonObjectId,
            NewBsonString,
            RegexToBsonRegularExpression,
            SByteToBsonBoolean,
            SByteToBsonDouble,
            SByteToBsonInt32,
            SByteToBsonInt64,
            SingleToBsonBoolean,
            SingleToBsonDouble,
            StringToBsonBoolean,
            StringToBsonDateTime,
            StringToBsonDouble,
            StringToBsonInt32,
            StringToBsonInt64,
            StringToBsonJavaScript,
            StringToBsonJavaScriptWithScope,
            StringToBsonObjectId,
            StringToBsonRegularExpression,
            StringToBsonSymbol,
            StringToBsonTimestamp,
            UInt16ToBsonBoolean,
            UInt16ToBsonDouble,
            UInt16ToBsonInt32,
            UInt16ToBsonInt64,
            UInt32ToBsonBoolean,
            UInt32ToBsonDouble,
            UInt32ToBsonInt32,
            UInt32ToBsonInt64,
            UInt64ToBsonBoolean,
            UInt64ToBsonDouble,
            UInt64ToBsonInt64,
            UInt64ToBsonTimestamp
        }

        private struct Mapping
        {
            private Type _netType;
            private BsonType _bsonType;

            public Mapping(Type netType, BsonType bsonType)
            {
                _netType = netType;
                _bsonType = bsonType;
            }

            public static Mapping FromTo(Type netType, BsonType bsonType)
            {
                return new Mapping(netType, bsonType);
            }

            public Type NetType { get { return _netType; } }
            public BsonType BsonType { get { return _bsonType; } }

            /// <summary>
            /// Compares this Mapping to another object.
            /// </summary>
            /// <param name="obj">The other object.</param>
            /// <returns>True if the other object is a Mapping and equal to this one.</returns>
            public override bool Equals(object obj)
            {
                Mapping rhs = (Mapping)obj;
                return _netType == rhs._netType && _bsonType == rhs._bsonType;
            }

            /// <summary>
            /// Gets the hash code.
            /// </summary>
            /// <returns>The hash code.</returns>
            public override int GetHashCode()
            {
                return _netType.GetHashCode() + (int)_bsonType;
            }
        }
    }
}
