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
    internal class BsonBinaryWriterContext
    {
        // private fields
        private BsonBinaryWriterContext _parentContext;
        private ContextType _contextType;
        private int _startPosition;
        private int _index; // used when contextType is Array

        // constructors
        internal BsonBinaryWriterContext(
            BsonBinaryWriterContext parentContext,
            ContextType contextType,
            int startPosition)
        {
            _parentContext = parentContext;
            _contextType = contextType;
            _startPosition = startPosition;
        }

        // internal properties
        internal BsonBinaryWriterContext ParentContext
        {
            get { return _parentContext; }
        }

        internal ContextType ContextType
        {
            get { return _contextType; }
        }

        internal int StartPosition
        {
            get { return _startPosition; }
        }

        internal int Index
        {
            get { return _index; }
            set { _index = value; }
        }
    }
}
