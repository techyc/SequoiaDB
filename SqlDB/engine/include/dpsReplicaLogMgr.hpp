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

   Source File Name = dpsReplicaLogMgr.hpp

   Descriptive Name = Data Protection Services Replica Log Manager Header

   When/how to use: this program may be used on binary and text-formatted
   versions of DPS component. This file contains code logic for replica manager

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/27/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef DPSREPLICALOGMGR_H_
#define DPSREPLICALOGMGR_H_

#include "core.hpp"
#include "oss.hpp"
#include "dpsLogPage.hpp"
#include "ossLatch.hpp"
#include "dpsMergeBlock.hpp"
#include "ossAtomic.hpp"
#include "dpsLogFileMgr.hpp"
#include "ossUtil.hpp"
#include "ossEvent.hpp"
#include "ossQueue.hpp"

namespace engine
{

   #define  DPS_SEARCH_MEM       0x01
   #define  DPS_SEARCH_FILE      0x10
   #define  DPS_SERCAH_ALL       (DPS_SEARCH_MEM|DPS_SEARCH_FILE)

   class _pmdEDUCB ;
   class dpsTransCB ;

   /*
      _dpsReplicaLogMgr define
   */
   class _dpsReplicaLogMgr : public SDBObject
   {
   private:
      ossQueue<_dpsLogPage *>    _queue;
      _dpsLogFileMgr             _logger;
      _dpsLogPage                *_pages;
      _ossSpinXLatch             _mtx;
      _ossAtomic32               _idleSize;
      DPS_LSN                    _lsn;
      DPS_LSN                    _currentLsn;
      UINT32                     _totalSize;
      UINT32                     _work;
      UINT32                     _begin ;
      BOOLEAN                    _rollFlag ;
      UINT32                     _pageNum;
      BOOLEAN                    _restoreFlag ;
      ossAutoEvent               _allocateEvent ;
      _ossAtomic32               _queSize ;

      dpsTransCB                 *_transCB ;
      dpsEventHandler            *_pEventHander ;

   public:
      _dpsReplicaLogMgr();

      ~_dpsReplicaLogMgr();

   public:
      OSS_INLINE UINT32 idleSize()
      {
         return _idleSize.peek();
      }

      OSS_INLINE DPS_LSN expectLsn()
      {
         DPS_LSN lsn ;
         _mtx.get();
         lsn = _lsn;
         _mtx.release();
         return lsn;
      }

      OSS_INLINE DPS_LSN currentLsn()
      {
         DPS_LSN lsn ;
         _mtx.get();
         lsn = _currentLsn;
         _mtx.release();
         return lsn;
      }

      OSS_INLINE DPS_LSN_VER incVersion()
      {
         DPS_LSN_VER version = DPS_INVALID_LSN_VERSION ;
         _mtx.get();
         version = ++_lsn.version ;
         _mtx.release();
         return version ;
      }

      OSS_INLINE void setEventHandler( dpsEventHandler *pEventHandler )
      {
         _pEventHander = pEventHandler ;
      }

      OSS_INLINE void unsetEventHandler()
      {
         _pEventHander = NULL ;
      }

   public:
      DPS_LSN getStartLsn ( BOOLEAN logBufOnly ) ;
      void getLsnWindow( DPS_LSN &fileBeginLsn,
                         DPS_LSN &memBeginLsn,
                         DPS_LSN &endLsn ) ;

      void getLsnWindow( DPS_LSN &fileBeginLsn,
                         DPS_LSN &memBeginLsn,
                         DPS_LSN &endLsn,
                         DPS_LSN &expected ) ;

      INT32 init( const CHAR *path, UINT32 pageNum );

      INT32 merge( _dpsMergeBlock &block ) ;

      INT32 preparePages ( dpsMergeInfo &info ) ;
      void  writeData ( dpsMergeInfo &info ) ;

      INT32 search( const DPS_LSN &minLsn, _dpsMessageBlock *mb,
                    UINT8 type, BOOLEAN onlyHeader );
      INT32 run( _pmdEDUCB *cb );
      INT32 tearDown();
      INT32 flushAll() ;

      INT32 checkSyncControl( UINT32 reqLen, _pmdEDUCB *cb ) ;

      INT32 move( const DPS_LSN_OFFSET &offset,
                  const DPS_LSN_VER &version ) ;

      void setLogFileSz ( UINT64 logFileSz )
      {
         _logger.setLogFileSz ( logFileSz ) ;
      }
      UINT32 getLogFileSz ()
      {
         return _logger.getLogFileSz () ;
      }
      void setLogFileNum ( UINT32 logFileNum )
      {
         _logger.setLogFileNum ( logFileNum ) ;
      }
      UINT32 getLogFileNum ()
      {
         return _logger.getLogFileNum () ;
      }

      BOOLEAN isInRestore ()
      {
         return _restoreFlag;
      }

      UINT32 calcFileID ( DPS_LSN_OFFSET offset )
      {
         return ( offset / getLogFileSz () ) % getLogFileNum () ;
      }

   private:
      void _allocate( UINT32 len,
                      dpsPageMeta &allocated ) ;
      void _push2SendQueue( const dpsPageMeta &allocated );
      void _mergeLogs( _dpsMergeBlock &block,
                       const dpsPageMeta &meta ) ;
      void _mergePage( const CHAR *src,
                       UINT32 len,
                       UINT32 &workSub,
                       UINT32 &offset );
      INT32 _flushPage( _dpsLogPage *page, BOOLEAN shutdown = FALSE );
      INT32 _flushAll() ;
      INT32 _search ( const DPS_LSN &lsn, _dpsMessageBlock *mb,
                      BOOLEAN onlyHeader ) ;
      DPS_LSN _getStartLsn () ;
      INT32 _parse( UINT32 sub, UINT32 offset, UINT32 len, CHAR *out ) ;

      INT32  _movePages ( const DPS_LSN_OFFSET &offset,
                          const DPS_LSN_VER &version ) ;
      INT32 _restore () ;

      UINT32 _decPageID ( UINT32 pageID )
      {
         return pageID ? pageID - 1 : _pageNum - 1 ;
      }
      UINT32 _incPageID ( UINT32 pageID )
      {
         ++pageID ;
         return pageID >= _pageNum ? 0 : pageID ;
      }
   };
   typedef class _dpsReplicaLogMgr dpsReplicaLogMgr;
}

#endif //DPSREPLICALOGMGR_H_

