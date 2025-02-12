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

   Source File Name = rtnLobFetcher.cpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/31/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "rtnLobFetcher.hpp"
#include "rtnTrace.hpp"

namespace engine
{
   _rtnLobFetcher::_rtnLobFetcher()
   :_suID( DMS_INVALID_CS ),
    _su( NULL ),
    _mbContext( NULL ),
    _pos( DMS_LOB_INVALID_PAGEID ),
    _onlyMetaPage( FALSE ),
    _hitEnd( FALSE )
   {

   }

   _rtnLobFetcher::~_rtnLobFetcher()
   {
      _fini() ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNLOBFETCHER_INIT, "_rtnLobFetcher::init" )
   INT32 _rtnLobFetcher::init( const CHAR *fullName,
                               BOOLEAN onlyMetaPage )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNLOBFETCHER_INIT ) ;
      SDB_DMSCB *dmsCB = sdbGetDMSCB() ;
      const CHAR *clName = NULL ;

      if ( NULL != _su || NULL != _mbContext )
      {
         PD_LOG( PDERROR, "do not init fetcher again" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = rtnResolveCollectionNameAndLock( fullName, dmsCB,
                                            &_su, &clName, _suID ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to resolve collection:%s, rc:%d",
                 fullName, rc ) ;
         goto error ;
      }

      rc = _su->data()->getMBContext( &_mbContext, clName, -1 ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to resolve collection name:%s",
                 clName ) ;
         goto error ;
      }

      _hitEnd = FALSE ;
      _pos = 0 ;
      _onlyMetaPage = onlyMetaPage ;
   done:
      PD_TRACE_EXITRC( SDB__RTNLOBFETCHER_INIT, rc ) ;
      return rc ;
   error:
      _fini() ;
      goto done ;      
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__RTNLOBFETCHER_FETCH, "_rtnLobFetcher::fetch" )
   INT32 _rtnLobFetcher::fetch( _pmdEDUCB *cb,
                                dmsLobInfoOnPage &page,
                                _dpsMessageBlock *mb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY( SDB__RTNLOBFETCHER_FETCH ) ;

      if ( _hitEnd )
      {
         rc = SDB_DMS_EOC ;
         goto error ;
      }

      if ( NULL == _mbContext || NULL == _su )
      {
         PD_LOG( PDERROR, "fetcher has not been initialized yet" ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = _mbContext->mbLock( SHARED ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get lock:%d", rc ) ;
         goto error ;
      }

      if ( !_su->lob()->isOpened() )
      {
         _hitEnd = TRUE ;
         rc = SDB_DMS_EOC ;
         goto error ;
      }

      rc = _su->lob()->readPage( _pos, _onlyMetaPage,
                                 cb, _mbContext, page ) ;
      if ( SDB_OK != rc )
      {
         if ( SDB_DMS_EOC == rc )
         {
            _hitEnd = TRUE ;
         }
         else
         {
            PD_LOG( PDERROR, "failed to read lob pages:%d", rc ) ;
         }
         goto error ;
      }

      if ( NULL != mb )
      {
         _dmsLobRecord record ;
         UINT32 read = 0 ;

         if ( mb->idleSize() < page._len )
         {
            rc = mb->extend( page._len - mb->idleSize() ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to extend mb block:%d", rc ) ;
               goto error ;
            }
         }

         record.set( &( page._oid ), page._sequence, 0,
                     page._len, NULL ) ;
         rc = _su->lob()->read( record, _mbContext,
                                cb, mb->writePtr(), read ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to read lob:%d", rc ) ;
            goto error ;
         }

         if ( page._len != read )
         {
            PD_LOG( PDERROR, "length in page is:%d, but we read:%d",
                    page._len, read ) ;
            rc = SDB_SYS ;
            goto error ;
         }

         mb->writePtr( mb->length() + read ) ;
      }
   done:
      if ( NULL != _mbContext && _mbContext->isMBLock() )
      {
         _mbContext->mbUnlock() ;
      }
      PD_TRACE_EXITRC( SDB__RTNLOBFETCHER_FETCH, rc ) ;
      return rc ;
   error:
      _fini() ;
      goto done ;
   }

   void _rtnLobFetcher::_fini()
   {
      SDB_DMSCB *dmsCB = sdbGetDMSCB() ;

      if ( NULL != _mbContext && NULL != _su )
      {
         if ( _mbContext->isMBLock() )
         {
            _mbContext->mbUnlock() ;
         }
         _su->data()->releaseMBContext( _mbContext ) ;
         _mbContext = NULL ;
      }
      if ( NULL != _su )
      {
         dmsCB->suUnlock ( _suID ) ;
         _su = NULL ;
         _suID = DMS_INVALID_CS ;
      }

      _onlyMetaPage = FALSE ;

      return ;
   }
}

