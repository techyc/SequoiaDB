/*******************************************************************************

   Copyright (C) 2012-2014 SqlDB Ltd.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   Source File Name = common.c

   Descriptive Name = C Common

   When/how to use: this program may be used on binary and text-formatted
   versions of C Client component. This file contains functions for
   client common functions ( including package build/extract ).

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include "common.h"
#include "ossMem.h"
#include "ossUtil.h"
#include "msgCatalogDef.h"
#include "oss.h"
#include "../bson/lib/md5.h"

#ifndef offsetof
#define offsetof(TYPE, MEMBER) ((size_t) &((TYPE *)0)->MEMBER)
#endif

static const INT32 clientDefaultVersion = 1 ;
static const INT16 clientDefaultW = 0 ;
static const UINT64 clientDefaultRouteID = 0 ;
static const SINT32 clientDefaultFlags = 0 ;
static INT32 clientCheckBuffer ( CHAR **ppBuffer, INT32 *bufferSize,
                           INT32 packetLength )
{
   INT32 rc = SDB_OK ;
   if ( packetLength > *bufferSize )
   {
      CHAR *pOld = *ppBuffer ;
      INT32 newSize = ossRoundUpToMultipleX ( packetLength, SDB_PAGE_SIZE ) ;
      if ( newSize < 0 )
      {
         ossPrintf ( "new buffer overflow"OSS_NEWLINE ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      *ppBuffer = (CHAR*)SDB_OSS_REALLOC ( *ppBuffer, sizeof(CHAR)*(newSize)) ;
      if ( !*ppBuffer )
      {
         ossPrintf ( "Failed to allocate %d bytes send buffer"OSS_NEWLINE,
                     newSize ) ;
         rc = SDB_OOM ;
         *ppBuffer = pOld ;
         goto error ;
      }
      *bufferSize = newSize ;
   }
done :
   return rc ;
error :
   goto done ;
}

static BOOLEAN bson_endian_convert ( CHAR *data, off_t *off, BOOLEAN l2r )
{
   INT32 objaftersize = 0 ;
   INT32 objbeforesize = *(INT32*)&data[*off] ;
   off_t beginOff = *off ;
   INT32 objrealsize = 0 ;
   ossEndianConvert4 ( objbeforesize, objaftersize ) ;
   *(INT32*)&data[*off] = objaftersize ;
   objrealsize = l2r?objbeforesize:objaftersize ;
   *off += sizeof(INT32) ;
   while ( BSON_EOO != data[*off] )
   {
      CHAR type = data[*off] ;
      *off += sizeof(CHAR) ;
      *off += ossStrlen ( &data[*off] ) + 1 ;
      switch ( type )
      {
      case BSON_DOUBLE :
      {
         FLOAT64 value = 0 ;
         ossEndianConvert8 ( *(FLOAT64*)&data[*off], value ) ;
         *(FLOAT64*)&data[*off] = value ;
         *off += sizeof(FLOAT64) ;
         break ;
      }
      case BSON_STRING :
      case BSON_CODE :
      case BSON_SYMBOL :
      {
         INT32 len = *(INT32*)&data[*off] ;
         INT32 newlen = 0 ;
         ossEndianConvert4 ( len, newlen ) ;
         *(INT32*)&data[*off] = newlen ;
         *off += sizeof(INT32) + (l2r?len:newlen) ;
         break ;
      }
      case BSON_OBJECT :
      case BSON_ARRAY :
      {
         BOOLEAN rc = bson_endian_convert ( data, off, l2r ) ;
         if ( !rc )
            return rc ;
         break ;
      }
      case BSON_BINDATA :
      {
         INT32 len = *(INT32*)&data[*off] ;
         INT32 newlen ;
         ossEndianConvert4 ( len, newlen ) ;
         *(INT32*)&data[*off] = newlen ;
         *off += sizeof(INT32) + sizeof(CHAR) + (l2r?len:newlen) ;
         break ;
      }
      case BSON_UNDEFINED :
      case BSON_NULL :
      case BSON_MINKEY :
      case BSON_MAXKEY :
      {
         break ;
      }
      case BSON_OID :
      {
         *off += 12 ;
         break ;
      }
      case BSON_BOOL :
      {
         *off += 1 ;
         break ;
      }
      case BSON_DATE :
      {
         INT64 date = *(INT64*)&data[*off] ;
         INT64 newdate = 0 ;
         ossEndianConvert8 ( date, newdate ) ;
         *(INT64*)&data[*off] = newdate ;
         *off += sizeof(INT64) ;
         break ;
      }
      case BSON_REGEX :
      {
         *off += ossStrlen ( &data[*off] ) + 1 ;
         *off += ossStrlen ( &data[*off] ) + 1 ;
         break ;
      }
      case BSON_DBREF :
      {
         INT32 len = *(INT32*)&data[*off] ;
         INT32 newlen = 0 ;
         ossEndianConvert4 ( len, newlen ) ;
         *(INT32*)&data[*off] = newlen ;
         *off += sizeof(INT32) + (l2r?len:newlen) ;
         *off += 12 ;
         break ;
      }
      case BSON_CODEWSCOPE :
      {
         INT32 value = 0 ;
         INT32 len, newlen ;
         BOOLEAN rc ;
         ossEndianConvert4 ( *(INT32*)&data[*off], value ) ;
         *(INT32*)&data[*off] = value ;
         *off += sizeof(INT32) ;
         len = *(INT32*)&data[*off] ;
         newlen = 0 ;
         ossEndianConvert4 ( len, newlen ) ;
         *(INT32*)&data[*off] = newlen ;
         *off += sizeof(INT32) + (l2r?len:newlen) ;
         rc = bson_endian_convert ( &data[*off], off, l2r ) ;
         if ( !rc )
            return rc ;
         break ;
      }
      case BSON_INT :
      {
         INT32 value = 0 ;
         ossEndianConvert4 ( *(INT32*)&data[*off], value ) ;
         *(INT32*)&data[*off] = value ;
         *off += sizeof(INT32) ;
         break ;
      }
      case BSON_TIMESTAMP :
      {
         INT32 value = 0 ;
         ossEndianConvert4 ( *(INT32*)&data[*off], value ) ;
         *(INT32*)&data[*off] = value ;
         *off += sizeof(INT32) ;
         ossEndianConvert4 ( *(INT32*)&data[*off], value ) ;
         *(INT32*)&data[*off] = value ;
         *off += sizeof(INT32) ;
         break ;
      }
      case BSON_LONG :
      {
         INT64 value = 0 ;
         ossEndianConvert8 ( *(INT64*)&data[*off], value ) ;
         *(INT64*)&data[*off] = value ;
         *off += sizeof(INT64) ;
         break ;
      }
      } // switch
   } // while ( BSON_EOO != data[*off] )
   *off += sizeof(CHAR) ;
   if ( *off - beginOff != objrealsize )
      return FALSE ;
   return TRUE ;
}

static void clientEndianConvertHeader ( MsgHeader *pHeader )
{
   MsgHeader newheader ;
   ossEndianConvert4 ( pHeader->messageLength, newheader.messageLength ) ;
   ossEndianConvert4 ( pHeader->opCode, newheader.opCode ) ;
   ossEndianConvert4 ( pHeader->TID, newheader.TID ) ;
   ossEndianConvert8 ( pHeader->routeID, newheader.routeID ) ;
   ossEndianConvert8 ( pHeader->requestID, newheader.requestID ) ;
   ossMemcpy ( pHeader, &newheader, sizeof(newheader) ) ;
}

INT32 clientCheckRetMsgHeader( const CHAR *pSendBuf, const CHAR *pRecvBuf )
{
   INT32 rc = SDB_OK ;
   INT32 sendOpCode = 0 ;
   UINT32 recvOpCode = 0 ;
   if ( NULL == pSendBuf || NULL == pRecvBuf )
   {
      rc = SDB_INVALIDARG ;
      goto done ;
   }
   sendOpCode = ((MsgHeader*)pSendBuf)->opCode ;
   recvOpCode = (UINT32)(((MsgHeader*)pRecvBuf)->opCode) ;
   if ( MAKE_REPLY_TYPE( sendOpCode ) != recvOpCode )
   {
      rc = SDB_UNEXPECTED_RESULT ;
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 clientBuildUpdateMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                             const CHAR *CollectionName, SINT32 flag,
                             UINT64 reqID,
                             bson *selector, bson *updator,
                             bson *hint, BOOLEAN endianConvert )
{
   bson emptyObj ;
   INT32 packetLength   = 0 ;
   INT32 rc             = SDB_OK ;
   MsgOpUpdate *pUpdate = NULL ;
   INT32 offset         = 0 ;
   INT32 nameLength     = 0 ;
   INT32 opCode         = MSG_BS_UPDATE_REQ ;
   UINT32 tid           = ossGetCurrentThreadID() ;
   bson_init ( &emptyObj ) ;
   bson_empty ( &emptyObj ) ;
   if ( !selector )
      selector = &emptyObj ;
   if ( !updator )
      updator = &emptyObj ;
   if ( !hint )
      hint = &emptyObj ;
   packetLength = ossRoundUpToMultipleX(offsetof(MsgOpUpdate, name) +
                                        ossStrlen ( CollectionName ) + 1,
                                        4 ) +
                        ossRoundUpToMultipleX( bson_size(selector), 4 ) +
                        ossRoundUpToMultipleX( bson_size(updator), 4 ) +
                        ossRoundUpToMultipleX( bson_size(hint), 4 ) ;
   if ( packetLength < 0 )
   {
      ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = clientCheckBuffer ( ppBuffer, bufferSize, packetLength ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }

   pUpdate = (MsgOpUpdate*)(*ppBuffer) ;
   ossEndianConvertIf ( flag, pUpdate->flags, endianConvert ) ;
   ossEndianConvertIf ( clientDefaultVersion, pUpdate->version,
                        endianConvert ) ;
   ossEndianConvertIf ( clientDefaultW, pUpdate->w, endianConvert ) ;
   nameLength = ossStrlen ( CollectionName ) ;
   ossEndianConvertIf ( nameLength, pUpdate->nameLength, endianConvert ) ;
   pUpdate->header.requestID           = reqID ;
   pUpdate->header.opCode              = opCode ;
   pUpdate->header.messageLength       = packetLength ;
   pUpdate->header.routeID.value       = clientDefaultRouteID ;
   pUpdate->header.TID                 = tid ;
   ossStrncpy ( pUpdate->name, CollectionName, nameLength ) ;
   pUpdate->name[nameLength] = 0 ;
   offset = ossRoundUpToMultipleX( offsetof(MsgOpUpdate, name) +
                                   nameLength + 1,
                                   4 ) ;
   if ( !endianConvert )
   {
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(selector),
                                          bson_size(selector));
      offset += ossRoundUpToMultipleX( bson_size(selector), 4 ) ;
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(updator),
                                          bson_size(updator));
      offset += ossRoundUpToMultipleX( bson_size(updator), 4 ) ;
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(hint), bson_size(hint));
      offset += ossRoundUpToMultipleX( bson_size(hint), 4 ) ;
   }
   else
   {

      bson newselector ;
      bson newupdator ;
      bson newhint ;
      off_t off = 0 ;
      clientEndianConvertHeader ( &pUpdate->header ) ;
      bson_init ( &newselector ) ;
      bson_init ( &newupdator ) ;
      bson_init ( &newhint ) ;
      bson_copy ( &newselector, selector ) ;
      bson_copy ( &newupdator, updator ) ;
      bson_copy ( &newhint, hint ) ;

      rc = bson_endian_convert ( (char*)bson_data(&newselector), &off, TRUE ) ;
      if ( rc == 0 )
      {
         goto endian_convert_done ;
      }
      off = 0 ;
      rc = bson_endian_convert ( (char*)bson_data(&newupdator), &off, TRUE ) ;
      if ( rc == 0 )
      {
         goto endian_convert_done ;
      }
      off = 0 ;
      rc = bson_endian_convert ( (char*)bson_data(&newhint), &off, TRUE ) ;
      if ( rc == 0 )
      {
         goto endian_convert_done ;
      }
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(&newselector),
                                          bson_size(selector));
      offset += ossRoundUpToMultipleX( bson_size(selector), 4 ) ;
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(&newupdator),
                                          bson_size(updator));
      offset += ossRoundUpToMultipleX( bson_size(updator), 4 ) ;
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(&newhint),
                                          bson_size(hint));
      offset += ossRoundUpToMultipleX( bson_size(hint), 4 ) ;

endian_convert_done :
      bson_destroy ( &newselector ) ;
      bson_destroy ( &newupdator ) ;
      bson_destroy ( &newhint ) ;
      if ( rc == FALSE )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else
         rc = SDB_OK ;
   }
   if ( offset != packetLength )
   {
      ossPrintf ( "Invalid packet length"OSS_NEWLINE ) ;
      rc = SDB_SYS ;
      goto error ;
   }
done :
   return rc ;
error :
   goto done ;
}

INT32 clientBuildUpdateMsgCpp ( CHAR **ppBuffer, INT32 *bufferSize,
                                const CHAR *CollectionName, SINT32 flag,
                                UINT64 reqID,
                                const CHAR*selector, const CHAR*updator,
                                const CHAR*hint, BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   bson bs ;
   bson bu ;
   bson bh ;
   bson_init ( &bs ) ;
   bson_init ( &bu ) ;
   bson_init ( &bh ) ;
   if ( selector )
   {
      bson_init_finished_data ( &bs, selector ) ;
   }
   if ( updator )
   {
      bson_init_finished_data ( &bu, updator ) ;
   }
   if ( hint )
   {
      bson_init_finished_data ( &bh, hint ) ;
   }
   rc = clientBuildUpdateMsg ( ppBuffer, bufferSize,
                               CollectionName, flag, reqID,
                               selector?&bs:NULL,
                               updator?&bu:NULL,
                               hint?&bh:NULL,
                               endianConvert ) ;
   bson_destroy ( &bs ) ;
   bson_destroy ( &bu ) ;
   bson_destroy ( &bh ) ;
   return rc ;
}

INT32 clientAppendInsertMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                              bson *insertor, BOOLEAN endianConvert )
{
   INT32 rc             = SDB_OK ;
   MsgOpInsert *pInsert = (MsgOpInsert*)(*ppBuffer) ;
   INT32 offset         = 0 ;
   INT32 packetLength   = 0 ;
   ossEndianConvertIf ( pInsert->header.messageLength, offset,
                         endianConvert ) ;
   packetLength   = offset +
                    ossRoundUpToMultipleX( bson_size(insertor), 4 ) ;
   if ( packetLength < 0 )
   {
      ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = clientCheckBuffer ( ppBuffer, bufferSize, packetLength ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }
   /* now the buffer is large enough */
   pInsert = (MsgOpInsert*)(*ppBuffer) ;
   ossEndianConvertIf ( packetLength, pInsert->header.messageLength,
                                      endianConvert ) ;
   if( !endianConvert )
   {
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(insertor),
                                          bson_size(insertor));
      offset += ossRoundUpToMultipleX ( bson_size(insertor), 4 ) ;
   }
   else
   {
      bson newinsertor ;
      off_t off = 0 ;
      bson_init ( &newinsertor ) ;
      bson_copy ( &newinsertor, insertor ) ;

      rc = bson_endian_convert ( (char*)bson_data(&newinsertor), &off, TRUE ) ;
      if ( rc == 0 )
      {
         goto endian_convert_done ;
      }
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(&newinsertor),
                  bson_size(insertor));
      offset += ossRoundUpToMultipleX( bson_size(insertor), 4 ) ;

endian_convert_done :
      bson_destroy ( &newinsertor ) ;
      if ( rc == FALSE )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else
         rc = SDB_OK ;
   }

   if ( offset != packetLength )
   {
      ossPrintf ( "Invalid packet length"OSS_NEWLINE ) ;
      rc = SDB_SYS ;
      goto error ;
   }
done :
   return rc ;
error :
   goto done ;

}

INT32 clientAppendInsertMsgCpp ( CHAR **ppBuffer, INT32 *bufferSize,
                                 const CHAR *insertor, BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   bson bi ;
   bson_init ( &bi ) ;
   bson_init_finished_data ( &bi, insertor ) ;
   rc = clientAppendInsertMsg ( ppBuffer, bufferSize,
                                &bi, endianConvert ) ;
   bson_destroy ( &bi ) ;
   return rc ;
}

INT32 clientBuildInsertMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                             const CHAR *CollectionName, SINT32 flag,
                             UINT64 reqID,
                             bson *insertor,
                             BOOLEAN endianConvert )
{
   INT32 rc             = SDB_OK ;
   MsgOpInsert *pInsert = NULL ;
   INT32 offset         = 0 ;
   INT32 nameLength     = 0 ;
   INT32 packetLength = ossRoundUpToMultipleX(offsetof(MsgOpInsert, name) +
                                              ossStrlen ( CollectionName ) + 1,
                                              4 ) +
                        ossRoundUpToMultipleX( bson_size(insertor), 4 ) ;
   if ( packetLength < 0 )
   {
      ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = clientCheckBuffer ( ppBuffer, bufferSize, packetLength ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }
   pInsert                       = (MsgOpInsert*)(*ppBuffer) ;
   ossEndianConvertIf ( clientDefaultVersion, pInsert->version,
                                              endianConvert ) ;
   ossEndianConvertIf ( clientDefaultW, pInsert->w, endianConvert ) ;
   ossEndianConvertIf ( flag, pInsert->flags, endianConvert ) ;
   nameLength = ossStrlen ( CollectionName ) ;
   ossEndianConvertIf( nameLength, pInsert->nameLength, endianConvert ) ;
   pInsert->header.requestID     = reqID ;
   pInsert->header.opCode        = MSG_BS_INSERT_REQ ;
   pInsert->header.messageLength = packetLength ;
   pInsert->header.routeID.value = 0 ;
   pInsert->header.TID           = ossGetCurrentThreadID() ;
   ossStrncpy ( pInsert->name, CollectionName, nameLength ) ;
   pInsert->name[nameLength]=0 ;
   offset = ossRoundUpToMultipleX( offsetof(MsgOpInsert, name) +
                                   nameLength + 1,
                                   4 ) ;
   if( !endianConvert )
   {
         ossMemcpy ( &((*ppBuffer)[offset]), bson_data(insertor),
                                             bson_size(insertor));
         offset += ossRoundUpToMultipleX( bson_size(insertor), 4 ) ;
   }
   else
   {
      bson newinsertor ;
      off_t off = 0 ;
      bson_init ( &newinsertor ) ;
      bson_copy ( &newinsertor, insertor ) ;
      clientEndianConvertHeader ( &pInsert->header ) ;
      rc = bson_endian_convert( (char*)bson_data(&newinsertor), &off, TRUE ) ;
      if ( rc == 0 )
      {
         goto endian_convert_done ;
      }
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(&newinsertor),
                                          bson_size(insertor));
      offset += ossRoundUpToMultipleX( bson_size(insertor), 4 ) ;
endian_convert_done :
      bson_destroy ( &newinsertor ) ;
      if ( rc == FALSE )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else
         rc = SDB_OK ;
   }
   if ( offset != packetLength )
   {
      ossPrintf ( "Invalid packet length"OSS_NEWLINE ) ;
      rc = SDB_SYS ;
      goto error ;
   }
done :
   return rc ;
error :
   goto done ;
}

INT32 clientBuildInsertMsgCpp ( CHAR **ppBuffer, INT32 *bufferSize,
                                const CHAR *CollectionName, SINT32 flag,
                                UINT64 reqID,
                                const CHAR *insertor,
                                BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   bson bi ;
   bson_init ( &bi ) ;
   bson_init_finished_data ( &bi, insertor ) ;
   rc = clientBuildInsertMsg ( ppBuffer, bufferSize, CollectionName, flag,
                               reqID, &bi, endianConvert ) ;
   bson_destroy ( &bi ) ;
   return rc ;
}

INT32 clientBuildQueryMsg  ( CHAR **ppBuffer, INT32 *bufferSize,
                             const CHAR *CollectionName, SINT32 flag,
                             UINT64 reqID,
                             SINT64 numToSkip, SINT64 numToReturn,
                             const bson *query, const bson *fieldSelector,
                             const bson *orderBy, const bson *hint,
                             BOOLEAN endianConvert )
{
   bson emptyObj ;
   INT32 packetLength   = 0 ;
   INT32 rc             = SDB_OK ;
   MsgOpQuery *pQuery   = NULL ;
   INT32 offset         = 0 ;
   INT32 nameLength     = 0 ;
   bson_init ( &emptyObj ) ;
   bson_empty ( &emptyObj ) ;
   if ( !query )
      query = &emptyObj ;
   if ( !fieldSelector )
      fieldSelector = &emptyObj ;
   if ( !orderBy )
      orderBy = &emptyObj ;
   if ( !hint )
      hint = &emptyObj ;
   packetLength = ossRoundUpToMultipleX(offsetof(MsgOpQuery, name) +
                                        ossStrlen ( CollectionName ) + 1,
                                        4 ) +
                        ossRoundUpToMultipleX( bson_size(query), 4 ) +
                        ossRoundUpToMultipleX( bson_size(fieldSelector), 4) +
                        ossRoundUpToMultipleX( bson_size(orderBy), 4 ) +
                        ossRoundUpToMultipleX( bson_size(hint), 4 ) ;
   if ( packetLength < 0 )
   {
      ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = clientCheckBuffer ( ppBuffer, bufferSize, packetLength ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }
   nameLength = ossStrlen ( CollectionName ) ;
   pQuery                        = (MsgOpQuery*)(*ppBuffer) ;
   ossEndianConvertIf ( clientDefaultVersion, pQuery->version,
                         endianConvert ) ;
   ossEndianConvertIf ( clientDefaultW, pQuery->w, endianConvert ) ;
   ossEndianConvertIf ( flag, pQuery->flags, endianConvert ) ;
   ossEndianConvertIf ( nameLength, pQuery->nameLength, endianConvert ) ;
   ossEndianConvertIf ( numToSkip, pQuery->numToSkip, endianConvert ) ;
   ossEndianConvertIf ( numToReturn, pQuery->numToReturn, endianConvert ) ;
   pQuery->header.requestID      = reqID ;
   pQuery->header.opCode         = MSG_BS_QUERY_REQ ;
   pQuery->header.messageLength  = packetLength ;
   pQuery->header.routeID.value  = 0 ;
   pQuery->header.TID            = ossGetCurrentThreadID() ;

   ossStrncpy ( pQuery->name, CollectionName, nameLength ) ;
   pQuery->name[nameLength]=0 ;
   offset = ossRoundUpToMultipleX( offsetof(MsgOpQuery, name) +
                                   nameLength + 1,
                                   4 ) ;

   if ( !endianConvert )
   {
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(query), bson_size(query)) ;
      offset += ossRoundUpToMultipleX( bson_size(query), 4 ) ;
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(fieldSelector),
                  bson_size(fieldSelector) ) ;
      offset += ossRoundUpToMultipleX( bson_size(fieldSelector), 4 ) ;
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(orderBy),
                  bson_size(orderBy) ) ;
      offset += ossRoundUpToMultipleX( bson_size(orderBy), 4 ) ;
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(hint),
                  bson_size(hint) ) ;
      offset += ossRoundUpToMultipleX( bson_size(hint), 4 ) ;
   }
   else
   {
      bson newquery ;
      bson newselector ;
      bson neworderby ;
      bson newhint ;
      off_t off = 0 ;
      clientEndianConvertHeader ( &pQuery->header ) ;
      bson_init ( &newquery ) ;
      bson_init ( &newselector ) ;
      bson_init ( &neworderby ) ;
      bson_init ( &newhint ) ;
      bson_copy ( &newquery, query ) ;
      bson_copy ( &newselector, fieldSelector ) ;
      bson_copy ( &neworderby, orderBy ) ;
      bson_copy ( &newhint, hint ) ;

      rc = bson_endian_convert ( (char*)bson_data(&newquery), &off, TRUE ) ;
      if ( rc == 0 )
      {
         goto endian_convert_done ;
      }
      off = 0 ;
      rc = bson_endian_convert ( (char*)bson_data(&newselector), &off, TRUE ) ;
      if ( rc == 0 )
      {
         goto endian_convert_done ;
      }
      off = 0 ;
      rc = bson_endian_convert ( (char*)bson_data(&neworderby), &off, TRUE ) ;
      if ( rc == 0 )
      {
         goto endian_convert_done ;
      }
      off = 0 ;
      rc = bson_endian_convert ( (char*)bson_data(&newhint), &off, TRUE ) ;
      if ( rc == 0 )
      {
         goto endian_convert_done ;
      }
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(&newquery),
                  bson_size(query));
      offset += ossRoundUpToMultipleX( bson_size(query), 4 ) ;
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(&newselector),
                  bson_size(fieldSelector));
      offset += ossRoundUpToMultipleX( bson_size(fieldSelector), 4 ) ;
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(&neworderby),
                  bson_size(orderBy));
      offset += ossRoundUpToMultipleX( bson_size(orderBy), 4 ) ;
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(&newhint),
                  bson_size(hint));
      offset += ossRoundUpToMultipleX( bson_size(hint), 4 ) ;

endian_convert_done :
      bson_destroy ( &newquery ) ;
      bson_destroy ( &newselector ) ;
      bson_destroy ( &neworderby ) ;
      bson_destroy ( &newhint ) ;
      if ( rc == FALSE )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else
         rc = SDB_OK ;
   }
   if ( offset != packetLength )
   {
      ossPrintf ( "Invalid packet length"OSS_NEWLINE ) ;
      rc = SDB_SYS ;
      goto error ;
   }
done :
   return rc ;
error :
   goto done ;
}

INT32 clientBuildQueryMsgCpp  ( CHAR **ppBuffer, INT32 *bufferSize,
                                const CHAR *CollectionName, SINT32 flag,
                                UINT64 reqID,
                                SINT64 numToSkip, SINT64 numToReturn,
                                const CHAR *query, const CHAR *fieldSelector,
                                const CHAR *orderBy, const CHAR *hint,
                                BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   bson bq ;
   bson bf ;
   bson bo ;
   bson bh ;
   bson_init ( &bq ) ;
   bson_init ( &bf ) ;
   bson_init ( &bo ) ;
   bson_init ( &bh ) ;
   if ( query )
   {
      bson_init_finished_data ( &bq, query ) ;
   }
   if ( fieldSelector )
   {
      bson_init_finished_data ( &bf, fieldSelector ) ;
   }
   if ( orderBy )
   {
      bson_init_finished_data ( &bo, orderBy ) ;
   }
   if ( hint )
   {
      bson_init_finished_data ( &bh, hint ) ;
   }
   rc = clientBuildQueryMsg ( ppBuffer, bufferSize,
                              CollectionName, flag, reqID,
                              numToSkip, numToReturn,
                              query?&bq:NULL,
                              fieldSelector?&bf:NULL,
                              orderBy?&bo:NULL,
                              hint?&bh:NULL,
                              endianConvert ) ;
   bson_destroy ( &bq ) ;
   bson_destroy ( &bf ) ;
   bson_destroy ( &bo ) ;
   bson_destroy ( &bh ) ;
   return rc ;
}

INT32 clientBuildGetMoreMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                              SINT32 numToReturn,
                              SINT64 contextID, UINT64 reqID,
                              BOOLEAN endianConvert )
{
   INT32 rc               = SDB_OK ;
   MsgOpGetMore *pGetMore = NULL ;
   INT32 packetLength = sizeof(MsgOpGetMore);
   if ( packetLength < 0 )
   {
      ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = clientCheckBuffer ( ppBuffer, bufferSize, packetLength ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }
   pGetMore                       = (MsgOpGetMore*)(*ppBuffer) ;
   ossEndianConvertIf ( numToReturn, pGetMore->numToReturn, endianConvert ) ;
   ossEndianConvertIf ( contextID, pGetMore->contextID, endianConvert ) ;
   pGetMore->header.requestID     = reqID ;
   pGetMore->header.opCode        = MSG_BS_GETMORE_REQ ;
   pGetMore->header.messageLength = packetLength ;
   pGetMore->header.routeID.value = 0 ;
   pGetMore->header.TID           = ossGetCurrentThreadID() ;
   if ( endianConvert )
   {
      clientEndianConvertHeader ( &pGetMore->header ) ;
   }
done :
   return rc ;
error :
   goto done ;
}

INT32 clientBuildDeleteMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                             const CHAR *CollectionName, SINT32 flag,
                             UINT64 reqID,
                             bson *deletor, bson *hint,
                             BOOLEAN endianConvert )
{
   bson emptyObj ;
   INT32 packetLength   = 0 ;
   INT32 rc             = SDB_OK ;
   MsgOpDelete *pDelete = NULL ;
   INT32 offset         = 0 ;
   INT32 nameLength     = 0 ;
   bson_init ( &emptyObj ) ;
   bson_empty ( &emptyObj ) ;
   if ( !deletor )
      deletor = &emptyObj ;
   if ( !hint )
      hint = &emptyObj ;
   packetLength = ossRoundUpToMultipleX(offsetof(MsgOpDelete, name) +
                                        ossStrlen ( CollectionName ) + 1,
                                        4 ) +
                        ossRoundUpToMultipleX( bson_size(deletor), 4 ) +
                        ossRoundUpToMultipleX( bson_size(hint), 4 ) ;
   if ( packetLength < 0 )
   {
      ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = clientCheckBuffer ( ppBuffer, bufferSize, packetLength ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }
   nameLength = ossStrlen ( CollectionName ) ;
   pDelete                       = (MsgOpDelete*)(*ppBuffer) ;
   ossEndianConvertIf ( clientDefaultVersion, pDelete->version, endianConvert ) ;
   ossEndianConvertIf ( clientDefaultW, pDelete->w, endianConvert ) ;
   ossEndianConvertIf ( flag, pDelete->flags, endianConvert ) ;
   ossEndianConvertIf ( nameLength, pDelete->nameLength, endianConvert ) ;
   pDelete->header.requestID     = reqID ;
   pDelete->header.opCode        = MSG_BS_DELETE_REQ ;
   pDelete->header.messageLength = packetLength ;
   pDelete->header.routeID.value = 0 ;
   pDelete->header.TID           = ossGetCurrentThreadID() ;
   ossStrncpy ( pDelete->name, CollectionName, nameLength ) ;
   pDelete->name[nameLength]=0 ;
   offset = ossRoundUpToMultipleX( offsetof(MsgOpDelete, name) +
                                   nameLength + 1,
                                   4 ) ;
   if( !endianConvert )
   {
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(deletor),
                                          bson_size(deletor) );
      offset += ossRoundUpToMultipleX( bson_size(deletor), 4 ) ;

      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(hint), bson_size(hint) );
      offset += ossRoundUpToMultipleX( bson_size(hint), 4 ) ;
   }
   else
   {
      bson newdeletor ;
      bson newhint ;
      off_t off = 0 ;
      clientEndianConvertHeader ( &pDelete->header ) ;
      bson_init ( &newdeletor ) ;
      bson_init ( &newhint ) ;
      bson_copy ( &newdeletor, deletor ) ;
      bson_copy ( &newhint, hint ) ;

      rc = bson_endian_convert ( (char*)bson_data(&newdeletor), &off, TRUE ) ;
      if ( rc == 0 )
      {
         goto endian_convert_done ;
      }
      off = 0 ;
      rc = bson_endian_convert ( (char*)bson_data(&newhint), &off, TRUE ) ;
      if ( rc == 0 )
      {
         goto endian_convert_done ;
      }
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(&newdeletor),
                  bson_size(deletor));
      offset += ossRoundUpToMultipleX( bson_size(deletor), 4 ) ;
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(&newhint),
                  bson_size(hint));
      offset += ossRoundUpToMultipleX( bson_size(hint), 4 ) ;
endian_convert_done :
      bson_destroy ( &newdeletor ) ;
      bson_destroy ( &newhint ) ;
      if ( rc == FALSE )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else
         rc = SDB_OK ;
   }
   if ( offset != packetLength )
   {
      ossPrintf ( "Invalid packet length"OSS_NEWLINE ) ;
      rc = SDB_SYS ;
      goto error ;
   }
done :
   return rc ;
error :
   goto done ;
}

INT32 clientBuildDeleteMsgCpp ( CHAR **ppBuffer, INT32 *bufferSize,
                                const CHAR *CollectionName, SINT32 flag,
                                UINT64 reqID,
                                const CHAR *deletor, const CHAR *hint,
                                BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   bson bd ;
   bson bh ;
   bson_init ( &bd ) ;
   bson_init ( &bh ) ;
   if ( deletor )
   {
      bson_init_finished_data ( &bd, deletor ) ;
   }
   if ( hint )
   {
      bson_init_finished_data ( &bh, hint ) ;
   }
   rc = clientBuildDeleteMsg ( ppBuffer, bufferSize,
                               CollectionName, flag, reqID,
                               deletor?&bd:NULL,
                               hint?&bh:NULL,
                               endianConvert ) ;
   bson_destroy ( &bd ) ;
   bson_destroy ( &bh ) ;
   return rc ;
}

INT32 clientBuildKillContextsMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                                   UINT64 reqID,
                                   SINT32 numContexts,
                                   const SINT64 *pContextIDs,
                                   BOOLEAN endianConvert )
{
   INT32 rc               = SDB_OK ;
   MsgOpKillContexts *pKC = NULL ;
   SINT32 i               = 0 ;
   INT32 packetLength = offsetof(MsgOpKillContexts, contextIDs) +
                        sizeof ( SINT64 ) * (numContexts) ;
   if ( packetLength < 0 )
   {
      ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = clientCheckBuffer ( ppBuffer, bufferSize, packetLength ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }
   pKC                       = (MsgOpKillContexts*)(*ppBuffer) ;
   pKC->header.requestID     = reqID ;
   pKC->header.opCode        = MSG_BS_KILL_CONTEXT_REQ ;
   pKC->header.messageLength = packetLength ;
   pKC->header.routeID.value = 0 ;
   pKC->header.TID           = ossGetCurrentThreadID() ;
   ossEndianConvertIf ( numContexts, pKC->numContexts, endianConvert ) ;
   if( endianConvert )
   {
      clientEndianConvertHeader ( &pKC->header ) ;
      for( ;i<numContexts;i++ )
      {
         ossEndianConvertIf ( pContextIDs[i], pKC->contextIDs[i],
                              endianConvert ) ;
      }
   }
   else
   {
       ossMemcpy ( (CHAR*)(&pKC->contextIDs[0]), (CHAR*)pContextIDs,
                    sizeof(SINT64)*pKC->numContexts ) ;
   }
done :
   return rc ;
error :
   goto done ;
}

INT32 clientBuildKillAllContextsMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                                      UINT64 reqID, BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   MsgOpKillAllContexts *killAllContexts = NULL ;
   INT32 len = sizeof( MsgOpKillAllContexts ) +
               ossRoundUpToMultipleX( 0, sizeof(ossValuePtr) ) ;
   if ( len < 0 )
   {
      ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = clientCheckBuffer ( ppBuffer, bufferSize, len ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }
   killAllContexts                       = (MsgOpKillAllContexts *)
                                           ( *ppBuffer ) ;
   killAllContexts->header.messageLength = len ;
   killAllContexts->header.opCode        = MSG_BS_INTERRUPTE ;
   killAllContexts->header.TID           = ossGetCurrentThreadID() ;
   killAllContexts->header.routeID.value = 0 ;
   killAllContexts->header.requestID     = reqID ;
   if ( endianConvert )
   {
      clientEndianConvertHeader ( &killAllContexts->header ) ;
   }
done :
   return rc ;
error :
   goto done ;
}

INT32 clientExtractReply ( CHAR *pBuffer, SINT32 *flag, SINT64 *contextID,
                           SINT32 *startFrom, SINT32 *numReturned,
                           BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   MsgOpReply *pReply = (MsgOpReply*)pBuffer ;
   ossEndianConvertIf ( pReply->flags, *flag, endianConvert ) ;
   ossEndianConvertIf ( pReply->contextID, *contextID, endianConvert ) ;
   ossEndianConvertIf ( pReply->startFrom, *startFrom, endianConvert ) ;
   ossEndianConvertIf ( pReply->numReturned, *numReturned, endianConvert ) ;
   if ( endianConvert )
   {
      INT32 offset, count ;
      clientEndianConvertHeader ( &pReply->header ) ;
      pReply->flags       = *flag ;
      pReply->contextID   = *contextID ;
      pReply->startFrom   = *startFrom ;
      pReply->numReturned = *numReturned ;
      offset = ossRoundUpToMultipleX( sizeof(MsgOpReply), 4 ) ;
      count = 0 ;
      while ( count < *numReturned && offset < pReply->header.messageLength )
      {
         off_t off = 0 ;
         rc = bson_endian_convert ( &pBuffer[offset], &off, FALSE ) ;
         if ( !rc )
         {
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         else
            rc = SDB_OK ;
         offset += *(INT32*)&pBuffer[offset] ;
         offset = ossRoundUpToMultipleX ( offset, 4 ) ;
         ++count ;
      }
   }
done :
   return rc ;
error :
   goto done ;
}

INT32 clientBuildDisconnectMsg ( CHAR **ppBuffer, INT32 *bufferSize,
                                 UINT64 reqID, BOOLEAN endianConvert )
{
   INT32 rc                     = SDB_OK ;
   MsgOpDisconnect *pDisconnect = NULL ;
   INT32 packetLength = ossRoundUpToMultipleX ( sizeof ( MsgOpDisconnect ),4) ;
   if ( packetLength < 0 )
   {
      ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = clientCheckBuffer ( ppBuffer, bufferSize, packetLength ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }
   pDisconnect                       = (MsgOpDisconnect*)(*ppBuffer) ;
   pDisconnect->header.requestID     = reqID ;
   pDisconnect->header.opCode        = MSG_BS_DISCONNECT ;
   pDisconnect->header.messageLength = packetLength ;
   pDisconnect->header.routeID.value = 0 ;
   pDisconnect->header.TID           = ossGetCurrentThreadID() ;
   if( endianConvert )
   {
      clientEndianConvertHeader ( &pDisconnect->header ) ;
   }
done :
   return rc ;
error :
   goto done ;
}

INT32 clientAppendOID ( bson *obj, bson_iterator *ret )
{
   INT32 rc = SDB_OK ;
   bson_iterator it ;
   rc = bson_find ( &it, obj, CLIENT_RECORD_ID_FIELD ) ;
   if ( BSON_EOO == rc )
   {
      /* if there's no "_id" exists in the object */
      bson_oid_t oid ;
      CHAR *data = NULL ;
      INT32 len  = 0 ;
      bson_oid_gen ( &oid ) ;
      /* 2 means type and '\0' for key */
      len = bson_size ( obj ) + CLIENT_RECORD_ID_FIELD_STRLEN +
            sizeof ( oid ) + 2 ;
      data = ( CHAR * ) malloc ( len ) ;
      if ( data )
      {
         CHAR *cur = data ;
         /* append size for whole object */
         *(INT32*)data = len ;
         data += sizeof(INT32) ;
         /* append type */
         *data = BSON_OID ;
         /* current position is where bson_oid starts, let's return it */
         if ( ret )
         {
            ret->cur = data ;
            ret->first = 0 ;
         }
         ++data ;
         /* append "_id" */
         data[0] = '_' ;
         data[1] = 'i' ;
         data[2] = 'd' ;
         data[3] = '\0' ;
         data += CLIENT_RECORD_ID_FIELD_STRLEN + 1 ;
         ossMemcpy ( data, oid.bytes, sizeof(oid ) ) ;
         data += sizeof ( oid ) ;
         ossMemcpy ( data, obj->data + sizeof ( INT32 ),
                     bson_size ( obj ) - sizeof ( INT32 ) ) ;
         if ( obj->ownmem )
            free ( obj->data ) ;
         obj->data = cur ;
         obj->cur = cur + len ;
      }
      else
      {
         rc = SDB_OOM ;
         goto error ;
      }
   }
   else if ( ret )
   {
      ossMemcpy ( ret, &it, sizeof(bson_iterator) ) ;
   }
   rc = SDB_OK ;
done :
   return rc ;
error :
   goto done ;
}

INT32 clientReplicaGroupExtractNode ( const CHAR *data,
                                      CHAR *pHostName,
                                      INT32 hostNameSize,
                                      CHAR *pServiceName,
                                      INT32 serviceNameSize,
                                      INT32 *pNodeID )
{
   INT32 rc = SDB_OK ;
   bson obj ;
   bson_iterator ele ;
   bson_init ( &obj ) ;

   if ( !data || !pHostName || !pServiceName || !pNodeID )
   {
      rc = SDB_INVALIDARG ;
      goto done ;
   }
   bson_init_finished_data ( &obj, (CHAR*)data ) ;
   if ( BSON_STRING == bson_find ( &ele, &obj, CAT_HOST_FIELD_NAME ) )
   {
      ossStrncpy ( pHostName, bson_iterator_string ( &ele ),
                   hostNameSize ) ;
   }

   if ( BSON_INT == bson_find ( &ele, &obj, CAT_NODEID_NAME ) )
   {
      *pNodeID = bson_iterator_int ( &ele ) ;
   }

   if ( BSON_ARRAY != bson_find ( &ele, &obj, CAT_SERVICE_FIELD_NAME ) )
   {
      rc = SDB_SYS ;
      goto error ;
   }
   {
      const CHAR *serviceList = bson_iterator_value ( &ele ) ;
      bson_iterator_from_buffer ( &ele, serviceList ) ;
      while ( bson_iterator_next ( &ele ) )
      {
         bson intObj ;
         bson_init ( &intObj ) ;
         if ( BSON_OBJECT == bson_iterator_type ( &ele ) &&
              BSON_OK == bson_init_finished_data ( &intObj,
                         (CHAR*)bson_iterator_value ( &ele ) ) )
         {
            bson_iterator k ;
            if ( BSON_INT != bson_find ( &k, &intObj,
                                         CAT_SERVICE_TYPE_FIELD_NAME ) )
            {
               rc = SDB_SYS ;
               bson_destroy ( &intObj ) ;
               goto error ;
            }
            if ( MSG_ROUTE_LOCAL_SERVICE == bson_iterator_int ( &k ) )
            {
               if ( BSON_STRING == bson_find ( &k, &intObj,
                    CAT_SERVICE_NAME_FIELD_NAME ) )
               {
                  ossStrncpy ( pServiceName,
                               bson_iterator_string ( &k ),
                               serviceNameSize ) ;
                  bson_destroy ( &intObj ) ;
                  goto done ;
               }
            }
         }
      }
   }
done :
   bson_destroy ( &obj ) ;
   return rc ;
error :
   goto done ;
}

INT32 clientBuildSqlMsg( CHAR **ppBuffer, INT32 *bufferSize,
                         const CHAR *sql, UINT64 reqID, BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   if ( NULL == sql )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   {
   INT32 sqlLen = ossStrlen( sql ) + 1 ;
   MsgOpSql *sqlMsg = NULL ;
   INT32 len = sizeof( MsgOpSql ) +
               ossRoundUpToMultipleX( sqlLen, sizeof(ossValuePtr) ) ;
   if ( len < 0 )
   {
      ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   rc = clientCheckBuffer ( ppBuffer, bufferSize, len ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }

   sqlMsg                       = ( MsgOpSql *)( *ppBuffer ) ;
   sqlMsg->header.requestID     = reqID ;
   sqlMsg->header.opCode        = MSG_BS_SQL_REQ ;
   sqlMsg->header.messageLength = sizeof( MsgOpSql ) + sqlLen ;
   sqlMsg->header.routeID.value = 0 ;
   sqlMsg->header.TID           = ossGetCurrentThreadID() ;
   ossMemcpy( *ppBuffer + sizeof( MsgOpSql ),
              sql, sqlLen ) ;
   if( endianConvert )
   {
      clientEndianConvertHeader ( &sqlMsg->header ) ;
   }
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 clientBuildAuthMsg( CHAR **ppBuffer, INT32 *bufferSize,
                          const CHAR *pUsrName,
                          const CHAR *pPasswd,
                          UINT64 reqID,
                          BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   INT32 msgLen = 0 ;
   bson obj ;
   INT32 bsonSize = 0 ;
   MsgAuthentication *msg ;
   if ( NULL == pUsrName ||
        NULL == pPasswd )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   bson_init( &obj ) ;
   bson_append_string( &obj, SDB_AUTH_USER, pUsrName ) ;
   bson_append_string( &obj, SDB_AUTH_PASSWD, pPasswd ) ;
   bson_finish( &obj ) ;

   bsonSize = bson_size( &obj ) ;
   msgLen = sizeof( MsgAuthentication ) +
            ossRoundUpToMultipleX( bsonSize, 4 ) ;
   if ( msgLen < 0 )
   {
      ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = clientCheckBuffer ( ppBuffer, bufferSize, msgLen ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }

   msg                       = ( MsgAuthentication * )(*ppBuffer) ;
   msg->header.requestID     = reqID ;
   msg->header.opCode        = MSG_AUTH_VERIFY_REQ ;
   msg->header.messageLength = sizeof( MsgAuthentication ) + bsonSize ;
   msg->header.routeID.value = 0 ;
   msg->header.TID           = ossGetCurrentThreadID() ;
   if ( !endianConvert )
   {
      ossMemcpy( *ppBuffer + sizeof(MsgAuthentication),
                 bson_data( &obj ), bsonSize ) ;
   }
   else
   {
      bson newobj ;
      off_t off = 0 ;
      clientEndianConvertHeader ( &msg->header ) ;
      bson_init ( &newobj ) ;
      bson_copy ( &newobj, &obj ) ;
      rc = bson_endian_convert ( (CHAR*)bson_data ( &newobj ) , &off, TRUE ) ;
      if ( !rc )
      {
         bson_destroy ( &newobj ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else
         rc = SDB_OK ;
      ossMemcpy( *ppBuffer + sizeof(MsgAuthentication),
                 bson_data( &newobj ), bsonSize ) ;
      bson_destroy ( &newobj ) ;
   }
done:
   bson_destroy( &obj ) ;
   return rc ;
error:
   goto done ;
}

INT32 clientBuildAuthCrtMsg( CHAR **ppBuffer, INT32 *bufferSize,
                             const CHAR *pUsrName,
                             const CHAR *pPasswd,
                             UINT64 reqID, BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   INT32 msgLen = 0 ;
   bson obj ;
   INT32 bsonSize = 0 ;
   MsgAuthCrtUsr *msg ;
   if ( NULL == pUsrName ||
        NULL == pPasswd )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   bson_init( &obj ) ;
   bson_append_string( &obj, SDB_AUTH_USER, pUsrName ) ;
   bson_append_string( &obj, SDB_AUTH_PASSWD, pPasswd ) ;
   bson_finish( &obj ) ;

   bsonSize = bson_size( &obj ) ;
   msgLen = sizeof( MsgAuthCrtUsr ) +
            ossRoundUpToMultipleX( bsonSize, sizeof(ossValuePtr ) ) ;
   if ( msgLen < 0 )
   {
      ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = clientCheckBuffer ( ppBuffer, bufferSize, msgLen ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }

   msg                       = ( MsgAuthCrtUsr * )(*ppBuffer) ;
   msg->header.requestID     = reqID ;
   msg->header.opCode        = MSG_AUTH_CRTUSR_REQ ;
   msg->header.messageLength = sizeof( MsgAuthCrtUsr ) + bsonSize ;
   msg->header.routeID.value = 0 ;
   msg->header.TID           = ossGetCurrentThreadID() ;

   if ( !endianConvert )
   {
      ossMemcpy( *ppBuffer + sizeof(MsgAuthCrtUsr),
                          bson_data( &obj ), bsonSize ) ;
   }
   else
   {
      bson newobj ;
      off_t off = 0 ;
      clientEndianConvertHeader ( &msg->header ) ;
      bson_init ( &newobj ) ;
      bson_copy ( &newobj, &obj ) ;

      rc = bson_endian_convert ( (char*)bson_data(&newobj), &off, TRUE ) ;
      if ( rc == 0 )
      {
         goto endian_convert_done ;
      }
      ossMemcpy( *ppBuffer + sizeof(MsgAuthCrtUsr),
                          bson_data( &newobj ), bsonSize ) ;
endian_convert_done :
      bson_destroy ( &newobj ) ;
      if ( rc == FALSE )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else
         rc = SDB_OK ;
   }
done:
   bson_destroy( &obj ) ;
   return rc ;
error:
   goto done ;
}

INT32 clientBuildAuthDelMsg( CHAR **ppBuffer, INT32 *bufferSize,
                             const CHAR *pUsrName,
                             const CHAR *pPasswd,
                             UINT64 reqID, BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   INT32 msgLen = 0 ;
   bson obj ;
   INT32 bsonSize = 0 ;
   MsgAuthDelUsr *msg ;
   if ( NULL == pUsrName ||
        NULL == pPasswd )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   bson_init( &obj ) ;
   bson_append_string( &obj, SDB_AUTH_USER, pUsrName ) ;
   bson_append_string( &obj, SDB_AUTH_PASSWD, pPasswd ) ;
   bson_finish( &obj ) ;

   bsonSize = bson_size( &obj ) ;
   msgLen = sizeof( MsgAuthDelUsr ) +
            ossRoundUpToMultipleX( bsonSize, sizeof(ossValuePtr ) ) ;
   if ( msgLen < 0 )
   {
      ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = clientCheckBuffer ( ppBuffer, bufferSize, msgLen ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }

   msg                       = ( MsgAuthDelUsr * )(*ppBuffer) ;
   msg->header.requestID     = reqID ;
   msg->header.opCode        = MSG_AUTH_DELUSR_REQ ;
   msg->header.routeID.value = 0 ;
   msg->header.TID           = ossGetCurrentThreadID() ;
   msg->header.messageLength = sizeof( MsgAuthDelUsr ) + bsonSize ;

  if ( !endianConvert )
   {
      ossMemcpy( *ppBuffer + sizeof(MsgAuthDelUsr),
                          bson_data( &obj ), bsonSize ) ;
   }
   else
   {
      bson newobj ;
      off_t off = 0 ;
      clientEndianConvertHeader ( &msg->header ) ;
      bson_init ( &newobj ) ;
      bson_copy ( &newobj, &obj ) ;

      rc = bson_endian_convert ( (char*)bson_data(&newobj), &off, TRUE ) ;
      if ( rc == 0 )
      {
         goto endian_convert_done ;
      }
      ossMemcpy( *ppBuffer + sizeof(MsgAuthDelUsr),
                          bson_data( &newobj ), bsonSize ) ;
endian_convert_done :
      bson_destroy ( &newobj ) ;
      if ( rc == FALSE )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else
         rc = SDB_OK ;
   }
done:
   bson_destroy( &obj ) ;
   return rc ;
error:
   goto done ;
}

INT32 clientBuildTransactionBegMsg( CHAR **ppBuffer, INT32 *bufferSize,
                                    UINT64 reqID, BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   MsgOpTransBegin *transBeginMsg = NULL ;
   INT32 len = sizeof( MsgOpTransBegin ) +
               ossRoundUpToMultipleX( 0, sizeof(ossValuePtr) ) ;
   if ( len < 0 )
   {
      ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   rc = clientCheckBuffer ( ppBuffer, bufferSize, len ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }

   transBeginMsg                       = ( MsgOpTransBegin *)( *ppBuffer ) ;
   transBeginMsg->header.requestID     = reqID ;
   transBeginMsg->header.opCode        = MSG_BS_TRANS_BEGIN_REQ ;
   transBeginMsg->header.messageLength = sizeof( MsgOpTransBegin ) + 0 ;
   transBeginMsg->header.routeID.value = 0 ;
   transBeginMsg->header.TID           = ossGetCurrentThreadID() ;
   if( endianConvert )
   {
      clientEndianConvertHeader ( &transBeginMsg->header ) ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 clientBuildTransactionCommitMsg( CHAR **ppBuffer, INT32 *bufferSize,
                             UINT64 reqID, BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   MsgOpTransCommit *transCommitMsg = NULL ;
   INT32 len = sizeof( MsgOpTransCommit ) +
               ossRoundUpToMultipleX( 0, sizeof(ossValuePtr) ) ;
   if ( len < 0 )
   {
      ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   rc = clientCheckBuffer ( ppBuffer, bufferSize, len ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }

   transCommitMsg                       = ( MsgOpTransCommit *)( *ppBuffer ) ;
   transCommitMsg->header.requestID     = reqID ;
   transCommitMsg->header.opCode        = MSG_BS_TRANS_COMMIT_REQ ;
   transCommitMsg->header.messageLength = sizeof( MsgOpTransBegin ) + 0 ;
   transCommitMsg->header.routeID.value = 0 ;
   transCommitMsg->header.TID           = ossGetCurrentThreadID() ;
   if( endianConvert )
   {
      clientEndianConvertHeader ( &transCommitMsg->header ) ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 clientBuildTransactionRollbackMsg( CHAR **ppBuffer, INT32 *bufferSize,
                             UINT64 reqID, BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   MsgOpTransRollback *transRollbackMsg = NULL ;
   INT32 len = sizeof( MsgOpTransRollback ) +
               ossRoundUpToMultipleX( 0, sizeof(ossValuePtr) ) ;
   if ( len < 0 )
   {
      ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   rc = clientCheckBuffer ( ppBuffer, bufferSize, len ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }

   transRollbackMsg                       = ( MsgOpTransRollback *)( *ppBuffer ) ;
   transRollbackMsg->header.requestID     = reqID ;
   transRollbackMsg->header.opCode        = MSG_BS_TRANS_ROLLBACK_REQ ;
   transRollbackMsg->header.messageLength = sizeof( MsgOpTransBegin ) + 0 ;
   transRollbackMsg->header.routeID.value = 0 ;
   transRollbackMsg->header.TID           = ossGetCurrentThreadID() ;
   if( endianConvert )
   {
      clientEndianConvertHeader ( &transRollbackMsg->header ) ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 md5Encrypt( const CHAR *src,
                  CHAR *code,
                  UINT32 size )
{
   INT32 rc                                  = 0 ;
   INT32 i                                   = 0 ;
   UINT8 md5digest [ SDB_MD5_DIGEST_LENGTH ] = {0} ;
   md5_state_t st ;

   /* sanity check */
   if ( size <= 2*SDB_MD5_DIGEST_LENGTH )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   md5_init ( &st ) ;
   md5_append ( &st, (const md5_byte_t *) src, ossStrlen(src) ) ;
   md5_finish ( &st, md5digest ) ;
   for ( ; i < SDB_MD5_DIGEST_LENGTH; i++ )
   {
      ossSnprintf( code, 3, "%02x", md5digest[i]) ;
      code += 2 ;
   }
done :
   return rc ;
error :
   goto done ;
}

INT32 clientBuildSysInfoRequest ( CHAR **ppBuffer, INT32 *pBufferSize )
{
   INT32 rc = SDB_OK ;
   MsgSysInfoRequest *request = NULL ;
   rc = clientCheckBuffer ( ppBuffer, pBufferSize, sizeof(MsgSysInfoRequest) ) ;
   if ( rc )
   {
      goto error ;
   }
   request                                   = (MsgSysInfoRequest*)(*ppBuffer) ;
   request->header.specialSysInfoLen         = MSG_SYSTEM_INFO_LEN ;
   request->header.eyeCatcher                = MSG_SYSTEM_INFO_EYECATCHER ;
   request->header.realMessageLength         = sizeof(MsgSysInfoRequest) ;
done :
   return rc ;
error :
   goto done ;
}

INT32 clientExtractSysInfoReply ( CHAR *pBuffer, BOOLEAN *endianConvert,
                                  INT32 *osType )
{
   INT32 rc = SDB_OK ;
   MsgSysInfoReply *reply = (MsgSysInfoReply*)pBuffer ;
   BOOLEAN e = FALSE ;
   if ( reply->header.eyeCatcher == MSG_SYSTEM_INFO_EYECATCHER )
   {
      e = FALSE ;
   }
   else if ( reply->header.eyeCatcher == MSG_SYSTEM_INFO_EYECATCHER_REVERT )
   {
      e = TRUE ;
   }
   else
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   if ( osType )
   {
      ossEndianConvertIf4(reply->osType, *osType, e ) ;
   }
   if ( endianConvert )
   {
      *endianConvert = e ;
   }
done :
   return rc ;
error :
   goto done ;
}

INT32 clientValidateSql( const CHAR *sql, BOOLEAN isExec )
{
   INT32 rc = SDB_INVALIDARG ;
   UINT32 size = ossStrlen( sql ) ;
   UINT32 i = 0 ;
   for ( ; i < size; i++ )
   {
      if ( ' ' == sql[i] || '\t' == sql[i] )
      {
         continue ;
      }
      else if ( isExec )
      {
         rc = ( 's' == sql[i] || 'S' == sql[i]
                || 'l' == sql[i] || 'L' == sql[i]) ?
              SDB_OK : SDB_INVALIDARG ;
         break ;
      }
      else
      {
         rc = ( 's' != sql[i] && 'S' != sql[i]
                 && 'l' != sql[i] && 'L' != sql[i]) ?
              SDB_OK : SDB_INVALIDARG ;
         break ;
      }
   }

   return rc ;
}

INT32 clientBuildAggrRequest1( CHAR **ppBuffer, INT32 *bufferSize,
                             const CHAR *CollectionName, bson **objs,
                             SINT32 num, BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   MsgOpAggregate *pAggr = NULL ;
   INT32 nameLength = 0;
   INT32 packetLength = 0;
   INT32 offset = 0;
   SINT32 i = 0;

   if ( NULL == objs || num <= 0 )
   {
      rc = SDB_INVALIDARG;
      ossPrintf( "param can't be empty!"OSS_NEWLINE );
      goto error;
   }
   nameLength = ossStrlen( CollectionName );
   packetLength = ossRoundUpToMultipleX( offsetof(MsgOpAggregate, name ) +
                                       nameLength + 1, 4 );
   for ( i = 0 ; i < num ; i++ )
   {
      packetLength += ossRoundUpToMultipleX( bson_size(objs[i]), 4 );
      if ( packetLength <= 0 )
      {
         rc = SDB_INVALIDARG;
         ossPrintf( "packet size overflow!"OSS_NEWLINE );
         goto error;
      }
   }
   rc = clientCheckBuffer( ppBuffer, bufferSize, packetLength );
   if ( rc )
   {
      ossPrintf( "Failed to check buffer, rc=%d"OSS_NEWLINE, rc );
      goto error;
   }
   pAggr = (MsgOpAggregate *)(*ppBuffer);
   ossEndianConvertIf( clientDefaultVersion, pAggr->version, endianConvert );
   ossEndianConvertIf( clientDefaultW, pAggr->w, endianConvert );
   ossEndianConvertIf(clientDefaultFlags, pAggr->flags, endianConvert );
   ossEndianConvertIf( nameLength, pAggr->nameLength, endianConvert );
   pAggr->header.messageLength = packetLength;
   pAggr->header.opCode = MSG_BS_AGGREGATE_REQ ;
   pAggr->header.routeID.value = 0;
   pAggr->header.requestID = 0;
   pAggr->header.TID = ossGetCurrentThreadID();
   ossStrncpy( pAggr->name, CollectionName, nameLength );
   pAggr->name[nameLength] = 0;
   offset = ossRoundUpToMultipleX( offsetof(MsgOpAggregate, name) +
                                 nameLength + 1, 4 );
   if ( !endianConvert )
   {
      for ( i = 0 ; i < num ; i++ )
      {
         ossMemcpy( &((*ppBuffer)[offset]), bson_data(objs[i]),
                  bson_size(objs[i]));
         offset += ossRoundUpToMultipleX( bson_size(objs[i]), 4 );
      }
   }
   else
   {
      clientEndianConvertHeader( &pAggr->header );
      for ( i = 0 ; i < num ; i++ )
      {
         off_t off = 0;
         bson newObj;
         bson_init( &newObj );
         bson_copy( &newObj, objs[i] );
         rc = bson_endian_convert((CHAR *)bson_data(&newObj), &off, TRUE );
         if ( 0 == rc )
         {
            goto endian_convert_done;
         }
         ossMemcpy(&((*ppBuffer)[offset]), bson_data(&newObj),
                  bson_size(objs[i]));
         offset += ossRoundUpToMultipleX( bson_size(objs[i]), 4 );
endian_convert_done:
         bson_destroy( &newObj );
         if ( FALSE == rc )
         {
            rc = SDB_INVALIDARG;
            goto error;
         }
         else
         {
            rc = SDB_OK;
         }
      }
   }
   if ( offset != packetLength )
   {
      ossPrintf( "Invalid packet length"OSS_NEWLINE );
      rc = SDB_SYS;
      goto error;
   }
done:
   return rc;
error:
   goto done;
}


INT32 clientBuildAggrRequest( CHAR **ppBuffer, INT32 *bufferSize,
                             const CHAR *CollectionName, bson *obj,
                             BOOLEAN endianConvert )
{
   INT32 rc             = SDB_OK ;
   MsgOpAggregate *pAggr = NULL ;
   INT32 offset         = 0 ;
   INT32 nameLength     = ossStrlen ( CollectionName ) ;
   INT32 packetLength = ossRoundUpToMultipleX(offsetof(MsgOpInsert, name) +
                                              nameLength + 1,
                                              4 ) +
                        ossRoundUpToMultipleX( bson_size(obj), 4 ) ;
   if ( packetLength < 0 )
   {
      ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = clientCheckBuffer ( ppBuffer, bufferSize, packetLength ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }
   pAggr = (MsgOpAggregate*)(*ppBuffer) ;
   ossEndianConvertIf ( clientDefaultVersion, pAggr->version,
                                              endianConvert ) ;
   ossEndianConvertIf ( clientDefaultW, pAggr->w, endianConvert ) ;
   ossEndianConvertIf ( clientDefaultFlags, pAggr->flags, endianConvert ) ;
   ossEndianConvertIf( nameLength, pAggr->nameLength, endianConvert ) ;
   pAggr->header.requestID     = 0 ;
   pAggr->header.opCode        = MSG_BS_AGGREGATE_REQ ;
   pAggr->header.messageLength = packetLength ;
   pAggr->header.routeID.value = 0 ;
   pAggr->header.TID           = ossGetCurrentThreadID() ;
   ossStrncpy ( pAggr->name, CollectionName, nameLength ) ;
   pAggr->name[nameLength]=0 ;
   offset = ossRoundUpToMultipleX( offsetof(MsgOpInsert, name) +
                                   nameLength + 1,
                                   4 ) ;
   if( !endianConvert )
   {
         ossMemcpy ( &((*ppBuffer)[offset]), bson_data(obj),
                                             bson_size(obj));
         offset += ossRoundUpToMultipleX( bson_size(obj), 4 ) ;
   }
   else
   {
      bson newinsertor ;
      off_t off = 0 ;
      bson_init ( &newinsertor ) ;
      bson_copy ( &newinsertor, obj ) ;
      clientEndianConvertHeader ( &pAggr->header ) ;
      rc = bson_endian_convert( (CHAR*)bson_data(&newinsertor), &off, TRUE ) ;
      if ( rc == 0 )
      {
         goto endian_convert_done ;
      }
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(&newinsertor),
                                          bson_size(obj));
      offset += ossRoundUpToMultipleX( bson_size(obj), 4 ) ;
endian_convert_done :
      bson_destroy ( &newinsertor ) ;
      if ( rc == FALSE )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else
         rc = SDB_OK ;
   }
   if ( offset != packetLength )
   {
      ossPrintf ( "Invalid packet length"OSS_NEWLINE ) ;
      rc = SDB_SYS ;
      goto error ;
   }
done :
   return rc ;
error :
   goto done ;
}

INT32 clientBuildAggrRequestCpp( CHAR **ppBuffer, INT32 *bufferSize,
                                const CHAR *CollectionName, const CHAR *obj,
                                BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   bson bi ;
   bson_init ( &bi ) ;
   bson_init_finished_data ( &bi, obj ) ;
   rc = clientBuildAggrRequest( ppBuffer, bufferSize,
                             CollectionName, &bi, endianConvert ) ;
   bson_destroy ( &bi ) ;
   return rc ;
}

INT32 clientAppendAggrRequest ( CHAR **ppBuffer, INT32 *bufferSize,
                              bson *obj, BOOLEAN endianConvert )
{
   INT32 rc             = SDB_OK ;
   MsgOpAggregate *pAggr = (MsgOpAggregate*)(*ppBuffer) ;
   INT32 offset         = 0 ;
   INT32 packetLength   = 0 ;
   ossEndianConvertIf ( pAggr->header.messageLength, offset,
                         endianConvert ) ;
   packetLength   = offset +
                    ossRoundUpToMultipleX( bson_size(obj), 4 ) ;
   if ( packetLength < 0 )
   {
      ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = clientCheckBuffer ( ppBuffer, bufferSize, packetLength ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }
   /* now the buffer is large enough */
   pAggr = (MsgOpAggregate*)(*ppBuffer) ;
   ossEndianConvertIf ( packetLength, pAggr->header.messageLength,
                                      endianConvert ) ;
   if( !endianConvert )
   {
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(obj),
                                          bson_size(obj));
      offset += ossRoundUpToMultipleX ( bson_size(obj), 4 ) ;
   }
   else
   {
      bson newinsertor ;
      off_t off = 0 ;
      bson_init ( &newinsertor ) ;
      bson_copy ( &newinsertor, obj ) ;

      rc = bson_endian_convert ( (char*)bson_data(&newinsertor), &off, TRUE ) ;
      if ( rc == 0 )
      {
         goto endian_convert_done ;
      }
      ossMemcpy ( &((*ppBuffer)[offset]), bson_data(&newinsertor),
                  bson_size(obj));
      offset += ossRoundUpToMultipleX( bson_size(obj), 4 ) ;

endian_convert_done :
      bson_destroy ( &newinsertor ) ;
      if ( rc == FALSE )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      else
         rc = SDB_OK ;
   }

   if ( offset != packetLength )
   {
      ossPrintf ( "Invalid packet length"OSS_NEWLINE ) ;
      rc = SDB_SYS ;
      goto error ;
   }
done :
   return rc ;
error :
   goto done ;

}


INT32 clientAppendAggrRequestCpp ( CHAR **ppBuffer, INT32 *bufferSize,
                                const CHAR *obj, BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   bson bi ;
   bson_init ( &bi ) ;
   bson_init_finished_data ( &bi, obj ) ;
   clientAppendAggrRequest ( ppBuffer, bufferSize,
                              &bi, endianConvert ) ;
   bson_destroy ( &bi ) ;
   return rc ;
}

INT32 clientBuildTestMsg( CHAR **ppBuffer, INT32 *bufferSize,
                          const CHAR *msg, UINT64 reqID,
                          BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   INT32 msgLen = 0 ;
   INT32 len = 0 ;
   MsgOpMsg *msgOpMsg = NULL ;
   msgLen = ossStrlen( msg ) + 1 ;
   len = sizeof( MsgOpMsg ) +
      ossRoundUpToMultipleX( msgLen, sizeof(ossValuePtr) ) ;
   if ( len < 0 )
   {
      ossPrintf( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }
   rc = clientCheckBuffer( ppBuffer, bufferSize, len ) ;
   if ( rc )
   {
      ossPrintf( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc );
      goto error ;
   }
   msgOpMsg = (MsgOpMsg*)(*ppBuffer) ;
   msgOpMsg->header.requestID     = reqID ;
   msgOpMsg->header.opCode        = MSG_BS_MSG_REQ ;
   msgOpMsg->header.messageLength = len ;
   msgOpMsg->header.routeID.value = 0 ;
   msgOpMsg->header.TID           = ossGetCurrentThreadID() ;
   ossMemcpy( msgOpMsg->msg, msg, msgLen ) ;
   if( endianConvert )
   {
      clientEndianConvertHeader ( &msgOpMsg->header ) ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 clientBuildLobMsg( CHAR **ppBuffer, INT32 *bufferSize,
                         INT32 msgType, const bson *meta,
                         SINT32 flags, SINT16 w, SINT64 contextID,
                         UINT64 reqID, const SINT64 *lobOffset,
                         const UINT32 *len, const CHAR *data,
                         BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   SINT32 packetLength = 0 ;
   MsgOpLob *msg = NULL ;
   SINT32 offset = 0 ;
   SINT16 padding = 0 ;
   UINT32 bsonLen = 0 ;
   MsgLobTuple *tuple = NULL ;

   packetLength = sizeof( MsgOpLob ) ;

   if ( NULL != meta )
   {
      bsonLen = bson_size( meta ) ;
      packetLength += ossRoundUpToMultipleX( bsonLen, 4 ) ;
   }


   if ( NULL != lobOffset && NULL != len )
   {
      packetLength += sizeof( MsgLobTuple ) ;
   }
   
   if ( NULL != data )
   {
      packetLength += ossRoundUpToMultipleX( *len, 4 ) ;
   }

   if ( packetLength < 0 )
   {
      ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   rc = clientCheckBuffer ( ppBuffer, bufferSize, packetLength ) ;
   if ( rc )
   {
      ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
      goto error ;
   }

   msg = ( MsgOpLob * )( *ppBuffer ) ;
   ossEndianConvertIf ( flags, msg->flags, endianConvert ) ;
   ossEndianConvertIf ( clientDefaultVersion, msg->version, endianConvert ) ;
   ossEndianConvertIf ( w, msg->w, endianConvert ) ;
   ossEndianConvertIf ( padding, msg->padding, endianConvert ) ;
   ossEndianConvertIf ( contextID, msg->contextID, endianConvert ) ;
   ossEndianConvertIf ( bsonLen, msg->bsonLen, endianConvert ) ;

   msg->header.requestID = reqID ;
   msg->header.opCode = msgType ;
   msg->header.messageLength = packetLength ;
   msg->header.routeID.value = clientDefaultRouteID ;
   msg->header.TID = ossGetCurrentThreadID() ;

   offset = sizeof( MsgOpLob ) ;

   if ( !endianConvert )
   {
      if ( NULL != meta )
      {
         ossMemcpy( *ppBuffer + offset, bson_data( meta ), bsonLen ) ;
         offset += ossRoundUpToMultipleX( bsonLen, 4 ) ;
      }

      if ( NULL != lobOffset && NULL != len )
      {
         tuple = ( MsgLobTuple * )(*ppBuffer + offset) ;
         tuple->columns.len = *len ;
         tuple->columns.offset = *lobOffset ;
         offset += sizeof( MsgLobTuple ) ;
      }

      if ( NULL != data )
      {
         ossMemcpy( *ppBuffer + offset, data, *len ) ;
         offset += ossRoundUpToMultipleX( *len, 4 ) ;
      }
   }
   else
   {
      BOOLEAN res = TRUE ;
      clientEndianConvertHeader ( &( msg->header ) ) ;
      if ( NULL != meta )
      {
         off_t off = 0 ;
         bson newObj ;
         bson_init( &newObj ) ;
         bson_copy( &newObj, meta ) ;
         res = bson_endian_convert ( (char*)bson_data(&newObj), &off, TRUE ) ;
         if ( rc )
         {
            ossMemcpy( *ppBuffer + offset, bson_data( &newObj ), bson_size( &newObj ) ) ;
            offset += ossRoundUpToMultipleX( bson_size( &newObj ), 4 ) ;
         }

         bson_destroy( &newObj ) ;
         if ( !res )
         {
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         else
         {
            rc = SDB_OK ;
         }
      }

      if ( NULL != lobOffset && NULL != len )
      {
         tuple = ( MsgLobTuple * )( *ppBuffer + offset ) ;
         ossEndianConvertIf( *len, tuple->columns.len, endianConvert ) ;
         ossEndianConvertIf( *lobOffset, tuple->columns.offset, endianConvert ) ;
         offset += sizeof( MsgLobTuple ) ;
      }

      if ( NULL != data )
      {
         ossMemcpy( *ppBuffer + offset, data, *len ) ;
         offset += ossRoundUpToMultipleX( *len, 4 ) ;
      }
   }

   if ( offset != packetLength )
   {
      ossPrintf ( "Invalid packet length"OSS_NEWLINE ) ;
      rc = SDB_SYS ;
      goto error ;
   }

done:
   return rc ;
error:
   goto done ;
}

INT32 clientBuildLobMsgCpp( CHAR **ppBuffer, INT32 *bufferSize,
                            INT32 msgType, const CHAR *pMeta,
                            SINT32 flags, SINT16 w, SINT64 contextID,
                            UINT64 reqID, const SINT64 *lobOffset,
                            const UINT32 *len, const CHAR *data,
                            BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   bson bi ;
   bson_init ( &bi ) ;
   bson_init_finished_data ( &bi, pMeta ) ;
   rc = clientBuildLobMsg( ppBuffer, bufferSize,
                           msgType, &bi,
                           flags, w, contextID,
                           reqID, lobOffset,
                           len, data,
                           endianConvert ) ;
   bson_destroy ( &bi ) ;
   return rc ;
}

INT32 clientBuildOpenLobMsg( CHAR **ppBuffer, INT32 *bufferSize,
                             const bson *meta, SINT32 flags, SINT16 w,
                             UINT64 reqID,
                             BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   rc = clientBuildLobMsg( ppBuffer, bufferSize,
                           MSG_BS_LOB_OPEN_REQ, meta,
                           flags, w, -1, reqID, NULL,
                           NULL, NULL, endianConvert ) ;

   if ( SDB_OK != rc )
   {
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 clientBuildOpenLobMsgCpp( CHAR **ppBuffer, INT32 *bufferSize,
                                const CHAR *pMeta, SINT32 flags, SINT16 w,
                                UINT64 reqID,
                                BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   rc = clientBuildLobMsgCpp( ppBuffer, bufferSize,
                              MSG_BS_LOB_OPEN_REQ, pMeta,
                              flags, w, -1, reqID, NULL,
                              NULL, NULL, endianConvert ) ;

   if ( SDB_OK != rc )
   {
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 clientBuildWriteLobMsg( CHAR **ppBuffer, INT32 *bufferSize,
                              const CHAR *buf, UINT32 len,
                              SINT64 lobOffset, SINT32 flags, SINT16 w,
                              SINT64 contextID, UINT64 reqID,
                              BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   rc = clientBuildLobMsg( ppBuffer, bufferSize,
                           MSG_BS_LOB_WRITE_REQ, NULL,
                           flags, w, contextID, reqID, &lobOffset,
                           &len, buf, endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 clientBuildReadLobMsg( CHAR **ppBuffer, INT32 *bufferSize,
                             UINT32 len, SINT64 lobOffset,
                             SINT32 flags, SINT64 contextID,
                             UINT64 reqID,
                             BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   rc = clientBuildLobMsg( ppBuffer, bufferSize,
                           MSG_BS_LOB_READ_REQ, NULL,
                           flags, 1, contextID, reqID,
                           &lobOffset, &len, NULL, endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 clientBuildCloseLobMsg( CHAR **ppBuffer, INT32 *bufferSize,
                              SINT32 flags, SINT16 w,
                              SINT64 contextID, UINT64 reqID,
                              BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   rc = clientBuildLobMsg( ppBuffer, bufferSize,
                           MSG_BS_LOB_CLOSE_REQ, NULL,
                           flags, w, contextID, reqID,
                           NULL, NULL, NULL, endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 clientBuildRemoveLobMsg( CHAR **ppBuffer, INT32 *bufferSize,
                               const bson *meta,
                               SINT32 flags, SINT16 w,
                               UINT64 reqID,
                               BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   rc = clientBuildLobMsg( ppBuffer, bufferSize,
                           MSG_BS_LOB_REMOVE_REQ, meta,
                           flags, w, -1, reqID,
                           NULL, NULL, NULL, endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;   
}

INT32 clientBuildRemoveLobMsgCpp( CHAR **ppBuffer, INT32 *bufferSize,
                                  const CHAR *pMeta,
                                  SINT32 flags, SINT16 w,
                                  UINT64 reqID,
                                  BOOLEAN endianConvert )
{
   INT32 rc = SDB_OK ;
   rc = clientBuildLobMsgCpp( ppBuffer, bufferSize,
                              MSG_BS_LOB_REMOVE_REQ, pMeta,
                              flags, w, -1, reqID,
                              NULL, NULL, NULL, endianConvert ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;   
}

