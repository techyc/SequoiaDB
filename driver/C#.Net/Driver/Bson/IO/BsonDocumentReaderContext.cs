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
using System.IO;
using System.Linq;
using System.Text;

namespace SqlDB.Bson.IO
{
    internal class BsonDocumentReaderContext
    {
        // private fields
        private BsonDocumentReaderContext _parentContext;
        private ContextType _contextType;
        private BsonDocument _document;
        private BsonArray _array;
        private int _index;

        // constructors
        internal BsonDocumentReaderContext(
            BsonDocumentReaderContext parentContext,
            ContextType contextType,
            BsonArray array)
        {
            _parentContext = parentContext;
            _contextType = contextType;
            _array = array;
        }

        internal BsonDocumentReaderContext(
            BsonDocumentReaderContext parentContext,
            ContextType contextType,
            BsonDocument document)
        {
            _parentContext = parentContext;
            _contextType = contextType;
            _document = document;
        }

        // used by Clone
        private BsonDocumentReaderContext(
            BsonDocumentReaderContext parentContext,
            ContextType contextType,
            BsonDocument document,
            BsonArray array,
            int index)
        {
            _parentContext = parentContext;
            _contextType = contextType;
            _document = document;
            _array = array;
            _index = index;
        }

        // internal properties
        internal BsonArray Array
        {
            get { return _array; }
        }

        internal ContextType ContextType
        {
            get { return _contextType; }
        }

        internal BsonDocument Document
        {
            get { return _document; }
        }

        internal int Index
        {
            get { return _index; }
            set { _index = value; }
        }

        // public methods
        /// <summary>
        /// Creates a clone of the context.
        /// </summary>
        /// <returns>A clone of the context.</returns>
        public BsonDocumentReaderContext Clone()
        {
            return new BsonDocumentReaderContext(_parentContext, _contextType, _document, _array, _index);
        }

        public BsonElement GetNextElement()
        {
            if (_index < _document.ElementCount)
            {
                return _document.GetElement(_index++);
            }
            else
            {
                return null;
            }
        }

        public BsonValue GetNextValue()
        {
            if (_index < _array.Count)
            {
                return _array[_index++];
            }
            else
            {
                return null;
            }
        }

        public BsonDocumentReaderContext PopContext()
        {
            return _parentContext;
        }
    }
}
