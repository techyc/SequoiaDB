/*******************************************************************************


   Copyright (C) 2011-2014 SqlDB Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the term of the GNU Affero General Public License, version 3,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warrenty of
   MARCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program. If not, see <http://www.gnu.org/license/>.

   Source File Name = utilParseCSV.cpp

   Descriptive Name =

   When/how to use: parse Jsons util

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/05/2013  JW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "pd.hpp"
#include "ossMem.hpp"
#include "ossUtil.hpp"
#include "utilParseData.hpp"
#include "pdTrace.hpp"
#include "utilTrace.hpp"
#include "text.h"

#define UTL_WORKER_CTJSIZE    512
#define UTL_WORKER_FIELD      "field"
#define UTL_DEFAULT_DELCHAR   '"'
#define UTL_CHAR_ESC          '\\'

PD_TRACE_DECLARE_FUNCTION ( SDB__UTILCSV__INIT, "_utilCSVParser::initialize" )
INT32 _utilCSVParser::initialize ( _utilParserParamet *parserPara )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB__UTILCSV__INIT );
   if ( parserPara->blockNum <= 0 )
   {
      rc = SDB_INVALIDARG ;
      PD_LOG ( PDERROR, "blockNum must be big than 0!" ) ;
      goto error ;
   }

   if ( parserPara->bufferSize < UTIL_DATA_BUFFER_SIZE ||
        parserPara->bufferSize % parserPara->blockNum )
   {
      rc = SDB_INVALIDARG ;
      PD_LOG ( PDERROR, "buffer space is too small 32MB!" ) ;
      goto error ;
   }

   _linePriority = parserPara->linePriority ;
   _bufferSize   = parserPara->bufferSize ;
   _blockNum     = parserPara->blockNum ;
   _blockSize    = _bufferSize / _blockNum ;
   _accessModel  = parserPara->accessModel ;
   mallocBufer ( _bufferSize ) ;
   if ( !_buffer )
   {
      rc = SDB_SYS ;
      PD_LOG ( PDERROR, "malloc error" ) ;
      goto error ;
   }
   _curBuffer = _buffer ;
   ossMemset ( _buffer, 0, _bufferSize ) ;
   if ( UTIL_GET_IO == _accessModel )
   {
      utilAccessParametLocalIO accessData ;
      accessData.pFileName = parserPara->fileName ;
      _pAccessData = SDB_OSS_NEW utilAccessDataLocalIO() ;
      if ( !_pAccessData )
      {
         rc = SDB_OOM ;
         PD_LOG ( PDERROR, "malloc error" ) ;
         goto error ;
      }
      rc = _pAccessData->initialize( (void*)&accessData ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to init IO,rc = %d", rc ) ;
         goto error ;
      }
   }
   else if ( UTIL_GET_HDFS == _accessModel )
   {
      utilAccessParametHdfs accessData ;
      accessData.pFileName = parserPara->fileName ;
      accessData.pPath     = parserPara->path ;
      accessData.pHostName = parserPara->hostName ;
      accessData.pUser     = parserPara->user ;
      accessData.port      = parserPara->port ;
      _pAccessData = SDB_OSS_NEW utilAccessDataHdfs() ;
      if ( !_pAccessData )
      {
         rc = SDB_OOM ;
         PD_LOG ( PDERROR, "malloc error" ) ;
         goto error ;
      }
      rc = _pAccessData->initialize( (void*)&accessData ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to init IO,rc = %d", rc ) ;
         goto error ;
      }
   }
   else
   {
      rc = SDB_INVALIDARG ;
      PD_LOG ( PDERROR, "Failed to access model,rc = %d", rc ) ;
      goto error ;
   }

   for ( INT32 i = 0; i < UTL_PARSER_CSV_CHAR_SPACE_NUM; ++i )
   {
      if ( _charSpace[i] == _delChar[0] ||
           _charSpace[i] == _delField[0] ||
           _charSpace[i] == _delRecord[0] )
      {
         _charSpace[i] = 0 ;
      }
   }

done:
   PD_TRACE_EXITRC ( SDB__UTILCSV__INIT, rc );
   return rc ;
error:
   goto done ;
}

CHAR *_utilCSVParser::getBuffer()
{
   return _buffer ;
}

PD_TRACE_DECLARE_FUNCTION ( SDB__UTILCSV__GETNEXTRECORD, "_utilCSVParser::getNextRecord" )
INT32 _utilCSVParser::getNextRecord ( UINT32 &startOffset,
                                      UINT32 &size,
                                      UINT32 *line,
                                      UINT32 *column,
                                      _bucket **ppBucket )
{
   INT32 rc = SDB_OK ;
   PD_TRACE_ENTRY ( SDB__UTILCSV__GETNEXTRECORD );
   BOOLEAN     isString       = FALSE ;
   UINT32      delCharNum     = 0 ;
   UINT32      newReadSize    = 0 ;
   UINT32      isReadSize     = 0 ;
   UINT32      useBlockNum    = 0 ;
   CHAR       *pCursor        = _curBuffer ;
   CHAR       *pReadBuffer     = NULL ;

   do
   {
      if ( 0 == _unreadSpace )
      {
         if ( _pBlock >= _blockNum )
         {
            BOOLEAN isFullBlock = FALSE ;
            isReadSize = pCursor - _curBuffer ;
            if ( isReadSize > _blockSize && isReadSize < _bufferSize )
            {
               _pBlock = 0 ;
               useBlockNum = ( (UINT32)( isReadSize / _blockSize ) ) ;
               if ( isReadSize % _blockSize > 0 )
               {
                  ++useBlockNum ;
               }
               while ( TRUE )
               {
                  if ( ppBucket )
                  {
                     ppBucket[_pBlock]->wait_to_get_exclusive_lock() ;
                  }
                  if( useBlockNum - 1 == 0 )
                  {
                     break ;
                  }
                  ++_pBlock ;
                  --useBlockNum ;
               }
               ossMemmove ( _buffer, _curBuffer, isReadSize ) ;
               _curBuffer = _buffer ;
               pReadBuffer = _buffer + isReadSize ;
               pCursor = pReadBuffer ;
               isFullBlock = ( isReadSize % _blockSize == 0 ) ;
               if( TRUE == isFullBlock )
               {
                  ++_pBlock ;
                  continue ;
               }
               newReadSize = _blockSize - ( isReadSize % _blockSize ) ;
            }
            else
            {
               if ( isReadSize == _bufferSize )
               {
                  isReadSize = 0 ;
                  PD_LOG ( PDWARNING, "Data size larger than the bucket "
                           "size %d, clear bucket data", _bufferSize ) ;
               }
               _pBlock = 0 ;
               if ( ppBucket )
               {
                  ppBucket[_pBlock]->wait_to_get_exclusive_lock() ;
               }
               ossMemmove ( _buffer, _curBuffer, isReadSize ) ;
               _curBuffer = _buffer ;
               pReadBuffer = _buffer + isReadSize ;
               pCursor = pReadBuffer ;
               newReadSize = _blockSize - isReadSize ;
               if ( newReadSize == 0 )
               {
                  ++_pBlock ;
                  continue ;
               }
            }
         }
         else
         {
            if ( ppBucket )
            {
               ppBucket[_pBlock]->wait_to_get_exclusive_lock() ;
            }
            newReadSize = _blockSize ;
            pReadBuffer = _buffer + _pBlock * _blockSize ;
         }
         rc = _pAccessData->readNextBuffer ( pReadBuffer, newReadSize ) ;
         if ( rc )
         {
            if ( rc == SDB_EOF && newReadSize == 0 )
            {
               startOffset = _curBuffer - _buffer ;
               size = pCursor - _curBuffer ;
               goto done ;
            }
            else if ( rc == SDB_EOF && newReadSize > 0 )
            {
               rc = SDB_OK ;
            }
            else
            {
               PD_LOG ( PDERROR, "Failed to read next buffer rc = %d", rc ) ;
               goto error ;
            }
         }
         _unreadSpace = newReadSize ;
         ++_pBlock ;
      }
      if ( _delChar[0] == *pCursor )
      {
         ++delCharNum ;
      }
      else if ( _delRecord[0] == *pCursor )
      {
         if ( delCharNum > 0 && delCharNum % 2 != 0 )
         {
            isString = !isString ;
         }
         delCharNum = 0 ;
         if ( !isString || _linePriority )
         {
            ++pCursor ;
            --_unreadSpace ;
            startOffset = _curBuffer - _buffer ;
            size = pCursor - _curBuffer ;
            _curBuffer = pCursor ;
            ++_line ;
            goto done ;
         }
      }
      else
      {
         if ( delCharNum > 0 && delCharNum % 2 != 0 )
         {
            isString = !isString ;
         }
         delCharNum = 0 ;
      }
      ++pCursor ;
      --_unreadSpace ;
   }while( TRUE ) ;
done:
   PD_TRACE_EXITRC ( SDB__UTILCSV__GETNEXTRECORD, rc );
   return rc ;
error:
   goto done ;
}

_utilCSVParser::_utilCSVParser() : _pBlock(0),
                                   _unreadSpace(0),
                                   _fieldSize(0),
                                   _readNumStr(0),
                                   _readFreeSpace(0),
                                   _leftFieldSize(0),
                                   _curBuffer(NULL),
                                   _fieldBuffer(NULL),
                                   _nextFieldCursor(NULL),
                                   _isString(FALSE),
                                   _delCharNum(0)
{
}

_utilCSVParser::~_utilCSVParser()
{
}
