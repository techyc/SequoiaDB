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

   Source File Name = ixm.cpp

   Descriptive Name = Index Manager

   When/how to use: this program may be used on binary and text-formatted
   versions of Index Manager component. This file contains functions for
   Index Manager Control Block.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "ixm.hpp"
#include "dmsStorageIndex.hpp"
#include "ixmIndexKey.hpp"
#include "ixmExtent.hpp"
#include "pdTrace.hpp"
#include "ixmTrace.hpp"

using namespace bson;

namespace engine
{
   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMINXCB1, "_ixmIndexCB::_ixmIndexCB" )
   _ixmIndexCB::_ixmIndexCB ( dmsExtentID extentID,
                              _dmsStorageIndex *pIndexSu,
                              _dmsContext *context )
   {
      SDB_ASSERT ( pIndexSu, "index su can't be NULL" ) ;
      PD_TRACE_ENTRY ( SDB__IXMINXCB1 );
      _isInitialized = FALSE ;
      _extent = (ixmIndexCBExtent*)(pIndexSu->extentAddr ( extentID )) ;
      _pIndexSu = pIndexSu ;
      _pContext = context ;
      _extentID = extentID ;
      _pageSize = _pIndexSu->pageSize () ;
      _init() ;
      PD_TRACE_EXIT( SDB__IXMINXCB1 );
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMINXCB2, "_ixmIndexCB::_ixmIndexCB" )
   _ixmIndexCB::_ixmIndexCB ( dmsExtentID extentID,
                              const BSONObj &infoObj,
                              UINT16 mbID ,
                              _dmsStorageIndex *pIndexSu,
                              _dmsContext *context )
   {
      SDB_ASSERT ( pIndexSu, "index su can't be NULL" ) ;
      PD_TRACE_ENTRY ( SDB__IXMINXCB2 );

      _isInitialized = FALSE ;
      _extent = (ixmIndexCBExtent*)(pIndexSu->extentAddr ( extentID )) ;
      _extent->_type = IXM_EXTENT_TYPE_NONE ;
      _pIndexSu = pIndexSu ;
      _pContext = context ;
      _extentID = extentID ;
      _pageSize = _pIndexSu->pageSize () ;

      if ( infoObj.objsize() + IXM_INDEX_CB_EXTENT_METADATA_SIZE >=
           (UINT32)_pageSize )
      {
         PD_LOG ( PDERROR, "index object is too big: %s",
                  infoObj.toString().c_str() ) ;
         goto error ;
      }

      _extent->_type = IXM_EXTENT_TYPE_NONE ;
      if ( !generateIndexType( infoObj, _extent->_type ) )
      {
         goto error ;
      }


      _extent->_flag           = DMS_EXTENT_FLAG_INUSE ;
      _extent->_eyeCatcher [0] = IXM_EXTENT_CB_EYECATCHER0 ;
      _extent->_eyeCatcher [1] = IXM_EXTENT_CB_EYECATCHER1 ;
      _extent->_indexFlag      = IXM_INDEX_FLAG_INVALID ;
      _extent->_mbID           = mbID ;
      _extent->_version        = DMS_EXTENT_CURRENT_V ;
      _extent->_logicID        = DMS_INVALID_EXTENT ;
      _extent->_scanExtLID     = DMS_INVALID_EXTENT ;
      _extent->_rootExtentID   = DMS_INVALID_EXTENT ;
      if ( !infoObj.hasField (DMS_ID_KEY_NAME) )
      {
         _IDToInsert oid ;
         oid._oid.init() ;
         *(INT32*)(((CHAR*)_extent) +IXM_INDEX_CB_EXTENT_METADATA_SIZE) =
               infoObj.objsize() + sizeof(_IDToInsert) ;
         ossMemcpy ( ((CHAR*)_extent) +
                     IXM_INDEX_CB_EXTENT_METADATA_SIZE +
                     sizeof(INT32),
                     (CHAR*)(&oid),
                     sizeof(_IDToInsert)) ;
         ossMemcpy ( ((CHAR*)_extent) +
                     IXM_INDEX_CB_EXTENT_METADATA_SIZE +
                     sizeof(INT32) +
                     sizeof(_IDToInsert),
                     infoObj.objdata()+sizeof(INT32),
                     infoObj.objsize()-sizeof(INT32) ) ;
      }
      else
      {
         ossMemcpy ( ((CHAR*)_extent) +
                     IXM_INDEX_CB_EXTENT_METADATA_SIZE,
                     infoObj.objdata(),
                     infoObj.objsize() ) ;
      }
      _init() ;

   done :
      PD_TRACE_EXIT ( SDB__IXMINXCB2 );
      return ;
   error :
      goto done ;
   }

   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMINXCB_GETKEY, "_ixmIndexCB::getKeysFromObject" )
   INT32 _ixmIndexCB::getKeysFromObject ( const BSONObj &obj,
                                          BSONObjSet &keys ) const
   {
      INT32 rc = SDB_OK ;
      SDB_ASSERT ( _isInitialized,
                   "index details must be initialized first" ) ;
      PD_TRACE_ENTRY ( SDB__IXMINXCB_GETKEY );
      ixmIndexKeyGen keyGen(this) ;
      rc = keyGen.getKeys ( obj, keys ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to generate key from object, rc: %d", rc ) ;
         goto error ;
      }

   done :
      PD_TRACE_EXITRC ( SDB__IXMINXCB_GETKEY, rc );
      return rc ;
   error :
      goto done ;
   }

   INT32 _ixmIndexCB::keyPatternOffset( const CHAR *key ) const
   {
      SDB_ASSERT ( _isInitialized,
                   "index details must be initialized first" ) ;
      BSONObjIterator i ( keyPattern() ) ;
      INT32 n = 0 ;
      while ( i.more() )
      {
         BSONElement e = i.next() ;
         if ( ossStrcmp ( key, e.fieldName() ) == 0 )
            return n ;
         n++ ;
      }
      return -1 ;
   }

   INT32 _ixmIndexCB::allocExtent ( dmsExtentID &extentID )
   {
      SDB_ASSERT ( _isInitialized,
                   "index details must be initialized first" ) ;
      return _pIndexSu->reserveExtent ( _extent->_mbID, extentID,
                                        _pContext ) ;
   }
   INT32 _ixmIndexCB::freeExtent ( dmsExtentID extentID )
   {
      return _pIndexSu->releaseExtent ( extentID ) ;
   }
   
   PD_TRACE_DECLARE_FUNCTION ( SDB__IXMINXCB_TRUNC, "_ixmIndexCB::truncate" )
   INT32 _ixmIndexCB::truncate ( BOOLEAN removeRoot )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__IXMINXCB_TRUNC );
      PD_TRACE1 ( SDB__IXMINXCB_TRUNC, PD_PACK_INT(removeRoot) );
      setFlag ( IXM_INDEX_FLAG_TRUNCATING ) ;
      dmsExtentID root = getRoot() ;
      if ( DMS_INVALID_EXTENT != root )
      {
         ixmExtent rootExtent ( root, _pIndexSu ) ;
         rootExtent.truncate ( this ) ;
         if ( removeRoot )
         {
            rc = freeExtent ( root ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to free extent %d", root ) ;
               goto error ;
            }
         }
      }
      setFlag ( IXM_INDEX_FLAG_NORMAL ) ;

   done :
      scanExtLID ( DMS_INVALID_EXTENT ) ;
      PD_TRACE_EXITRC ( SDB__IXMINXCB_TRUNC, rc );
      return rc ;
   error :
      setFlag ( IXM_INDEX_FLAG_INVALID ) ;
      goto done ;
   }

   //PD_TRACE_DECLARE_FUNCTION ( SDB__IXMINXCB_ISSAMEDEF, "_ixmIndexCB::isSameDef" )
   BOOLEAN _ixmIndexCB::isSameDef( const BSONObj &defObj )
   {
      BOOLEAN rs = TRUE;
      SDB_ASSERT( TRUE == _isInitialized, "indexCB must be intialized!" );
      try
      {
         BSONElement beLKey = _infoObj.getField( IXM_KEY_FIELD );
         BSONElement beRKey = defObj.getField( IXM_KEY_FIELD );
         if ( 0 != beLKey.woCompare( beRKey, false ) )
         {
            rs = FALSE;
            goto done;
         }

         BOOLEAN lIsUnique = FALSE;
         BOOLEAN rIsUnique = FALSE;
         BSONElement beLUnique = _infoObj.getField( IXM_UNIQUE_FIELD );
         BSONElement beRUnique = defObj.getField( IXM_UNIQUE_FIELD );
         if ( beLUnique.booleanSafe() )
         {
            lIsUnique = TRUE;
         }
         if ( beRUnique.booleanSafe() )
         {
            rIsUnique = TRUE;
         }
         if ( lIsUnique != rIsUnique )
         {
            rs = FALSE;
            goto done;
         }

         BOOLEAN lEnforced = FALSE;
         BOOLEAN rEnforced = FALSE;
         BSONElement beLEnforced = _infoObj.getField( IXM_ENFORCED_FIELD );
         BSONElement beREnforced = defObj.getField( IXM_ENFORCED_FIELD );
         if ( beLEnforced.booleanSafe() )
         {
            lEnforced = TRUE;
         }
         if ( beREnforced.booleanSafe() )
         {
            rEnforced = TRUE;
         }
         if ( lEnforced != rEnforced )
         {
            rs = FALSE;
            goto done;
         }
      }
      catch( std::exception &e )
      {
         rs = FALSE;
         PD_LOG( PDERROR, "occur unexpected error(%s)", e.what() );
      }

   done:
      return rs;
   }


}

