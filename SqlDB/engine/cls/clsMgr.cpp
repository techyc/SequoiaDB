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

   Source File Name = clsMgr.cpp

   Descriptive Name = Data Management Service Header

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          29/11/2012  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#include "clsMgr.hpp"
#include "msgMessage.hpp"
#include "pmd.hpp"
#include "clsShardSession.hpp"
#include "clsReplSession.hpp"
#include "clsFSDstSession.hpp"
#include "clsFSSrcSession.hpp"
#include "../bson/bson.h"
#include "pdTrace.hpp"
#include "clsTrace.hpp"
#include "dpsOp2Record.hpp"
#include "pmdStartup.hpp"

using namespace bson ;

namespace engine
{

   #define MAX_SHD_SESSION_CATCH_DEQ_SIZE          (1000)

   #define CLS_WAIT_CB_ATTACH_TIMEOUT              ( 300 * OSS_ONE_SEC )


   /*
      _clsShardSessionMgr implement
   */
   _clsShardSessionMgr::_clsShardSessionMgr( _clsMgr *pClsMgr )
   {
      _pClsMgr    = pClsMgr ;
      _unShardSessionTimer = NET_INVALID_TIMER_ID ;
   }

   _clsShardSessionMgr::~_clsShardSessionMgr()
   {
      _pClsMgr    = NULL ;
   }

   BOOLEAN _clsShardSessionMgr::isUnShardTimerStarted() const
   {
      return NET_INVALID_TIMER_ID == _unShardSessionTimer ?
             FALSE : TRUE ;
   }

   void _clsShardSessionMgr::startUnShardTimer( UINT32 interval )
   {
      if ( _pRTAgent && _pTimerHandle && !isUnShardTimerStarted() )
      {
         _pRTAgent->addTimer( interval, _pTimerHandle,
                              _unShardSessionTimer ) ;
      }
   }

   void _clsShardSessionMgr::stopUnShardTimer()
   {
      if ( _pRTAgent && _pTimerHandle && isUnShardTimerStarted() )
      {
         _pRTAgent->removeTimer( _unShardSessionTimer ) ;
         _unShardSessionTimer = NET_INVALID_TIMER_ID ;
      }
   }

   UINT64 _clsShardSessionMgr::makeSessionID( const NET_HANDLE &handle,
                                              const MsgHeader *header )
   {
      UINT64 sessionID = ossPack32To64( header->routeID.columns.nodeID,
                                        header->TID ) ;
      if ( header->routeID.columns.nodeID < DATA_NODE_ID_BEGIN ||
           header->routeID.columns.groupID < DATA_GROUP_ID_BEGIN )
      {
         sessionID = ossPack32To64( PMD_BASE_HANDLE_ID + handle, header->TID ) ;
      }

      return sessionID ;
   }

   SDB_SESSION_TYPE _clsShardSessionMgr::_prepareCreate( UINT64 sessionID,
                                                         INT32 startType,
                                                         INT32 opCode )
   {
      SDB_SESSION_TYPE sessionType = SDB_SESSION_MAX ;
      UINT32 nodeID = 0 ;
      UINT32 tid = 0 ;

      ossUnpack32From64( sessionID, nodeID, tid ) ;
      if ( PMD_BASE_HANDLE_ID >= nodeID )
      {
         if ( PMD_SESSION_ACTIVE == startType )
         {
            sessionType = SDB_SESSION_SPLIT_DST ;
         }
         else
         {
            sessionType = SDB_SESSION_SPLIT_SRC ;
         }
      }
      else
      {
         sessionType = SDB_SESSION_SHARD ;
      }

      return sessionType ;
   }

   BOOLEAN _clsShardSessionMgr::_canReuse( SDB_SESSION_TYPE sessionType )
   {
      if ( SDB_SESSION_SHARD == sessionType )
      {
         return TRUE ;
      }
      return FALSE ;
   }

   UINT32 _clsShardSessionMgr::_maxCacheSize() const
   {
      return MAX_SHD_SESSION_CATCH_DEQ_SIZE ;
   }

   void _clsShardSessionMgr::_onPushMsgFailed( INT32 rc, const MsgHeader *pReq,
                                               const NET_HANDLE &handle,
                                               pmdAsyncSession *pSession )
   {
      if ( MSG_INVALID_ROUTEID == pReq->routeID.value )
      {
         _reply( handle, rc, pReq ) ;
      }
   }

   pmdAsyncSession* _clsShardSessionMgr::_createSession(
         SDB_SESSION_TYPE sessionType,
         INT32 startType,
         UINT64 sessionID,
         void *data )
   {
      pmdAsyncSession *pSession = NULL ;

      if ( SDB_SESSION_SPLIT_DST == sessionType )
      {
         pSession = SDB_OSS_NEW _clsSplitDstSession ( sessionID, _pRTAgent,
                                                      data ) ;
      }
      else if ( SDB_SESSION_SPLIT_SRC == sessionType )
      {
         pSession = SDB_OSS_NEW _clsSplitSrcSession ( sessionID, _pRTAgent ) ;
      }
      else if ( SDB_SESSION_SHARD == sessionType )
      {
         pSession = SDB_OSS_NEW _clsShdSession ( sessionID ) ;
      }
      else
      {
         PD_LOG( PDERROR, "Unknow session type[%d]", sessionType ) ;
      }

      return pSession ;
   }

   INT32 _clsShardSessionMgr::handleSessionTimeout( UINT32 timerID,
                                                    UINT32 interval )
   {
      INT32 rc = SDB_OK ;

      if ( _unShardSessionTimer == timerID )
      {
         _checkUnShardSessions( interval ) ;

         _pClsMgr->_startInnerSession( CLS_SHARD, this ) ;

         goto done ;
      }
      else if ( _sessionTimerID == timerID )
      {
         if ( _mapSession.size() <= _maxCacheSize() / 10 )
         {
            goto done ;
         }
      }

      rc = _pmdAsycSessionMgr::handleSessionTimeout( timerID, interval ) ;

   done:
      return rc ;
   }

   void _clsShardSessionMgr::_checkUnShardSessions( UINT32 interval )
   {
      pmdAsyncSession *pSession = NULL ;
      MAPSESSION_IT it = _mapSession.begin() ;
      while ( it != _mapSession.end() )
      {
         pSession = it->second ;
         if ( SDB_SESSION_SHARD == pSession->sessionType() )
         {
            ++it ;
            continue ;
         }
         if ( !pSession->isProcess() && pSession->timeout( interval ) )
         {
            PD_LOG ( PDEVENT, "Session[%s] timeout", pSession->sessionName() ) ;
            _releaseSession_i ( pSession, TRUE, TRUE ) ;
            _mapSession.erase( it++ ) ;
            continue ;
         }
         ++it ;
      }
   }

   /*
      _clsReplSessionMgr implement
   */
   _clsReplSessionMgr::_clsReplSessionMgr( _clsMgr *pClsMgr )
   {
      _pClsMgr = pClsMgr ;
   }

   _clsReplSessionMgr::~_clsReplSessionMgr()
   {
      _pClsMgr = NULL ;
   }

   INT32 _clsReplSessionMgr::handleSessionTimeout( UINT32 timerID,
                                                   UINT32 interval )
   {
      INT32 rc = SDB_OK ;

      rc = _pmdAsycSessionMgr::handleSessionTimeout( timerID, interval ) ;
      if ( SDB_OK == rc )
      {
         _pClsMgr->_startInnerSession( CLS_REPL, this ) ;
      }

      return rc ;
   }

   UINT64 _clsReplSessionMgr::makeSessionID( const NET_HANDLE & handle,
                                             const MsgHeader * header )
   {
      return ossPack32To64( header->routeID.columns.nodeID,
                            header->TID ) ;
   }

   SDB_SESSION_TYPE _clsReplSessionMgr::_prepareCreate( UINT64 sessionID,
                                                        INT32 startType,
                                                        INT32 opCode )
   {
      SDB_SESSION_TYPE sessionType = SDB_SESSION_MAX ;
      UINT32 nodeID = 0 ;
      UINT32 tid = 0 ;

      ossUnpack32From64( sessionID, nodeID, tid ) ;

      if ( CLS_TID_REPL_SYC == tid )
      {
         sessionType = PMD_SESSION_ACTIVE == startType ?
                       SDB_SESSION_REPL_DST :
                       SDB_SESSION_REPL_SRC ;
      }
      else if ( CLS_TID_REPL_FS_SYC == tid )
      {
         if ( PMD_SESSION_ACTIVE == startType )
         {
            sessionType = SDB_SESSION_FS_DST ;
         }
         else
         {
            sessionType = SDB_SESSION_FS_SRC ;
         }
      }

      return sessionType ;
   }

   BOOLEAN _clsReplSessionMgr::_canReuse( SDB_SESSION_TYPE sessionType )
   {
      return FALSE ;
   }

   UINT32 _clsReplSessionMgr::_maxCacheSize() const
   {
      return 0 ;
   }

   void _clsReplSessionMgr::_onPushMsgFailed( INT32 rc, const MsgHeader *pReq,
                                              const NET_HANDLE &handle,
                                              pmdAsyncSession *pSession )
   {
   }

   pmdAsyncSession* _clsReplSessionMgr::_createSession(
         SDB_SESSION_TYPE sessionType,
         INT32 startType,
         UINT64 sessionID,
         void *data )
   {
      pmdAsyncSession *pSession = NULL ;
      if ( SDB_SESSION_REPL_DST == sessionType )
      {
         pSession = SDB_OSS_NEW clsReplDstSession( sessionID ) ;
      }
      else if ( SDB_SESSION_REPL_SRC == sessionType )
      {
         UINT32 nodeID = 0 ;
         UINT32 tid = 0 ;
         ossUnpack32From64( sessionID, nodeID, tid ) ;

         if ( pmdGetNodeID().columns.nodeID != nodeID )
         {
            pSession = SDB_OSS_NEW clsReplSrcSession( sessionID ) ;
         }
      }
      else if ( SDB_SESSION_FS_DST == sessionType )
      {
         pSession = SDB_OSS_NEW _clsFSDstSession ( sessionID,
                                                   _pRTAgent ) ;
      }
      else if ( SDB_SESSION_FS_SRC == sessionType )
      {
         pSession = SDB_OSS_NEW _clsFSSrcSession ( sessionID,
                                                   _pRTAgent ) ;
      }
      else
      {
         PD_LOG( PDERROR, "Unknow session type[%d]", sessionType ) ;
      }

      return pSession ;
   }

   /*
      _clsMgr implement
   */
   BEGIN_OBJ_MSG_MAP( _clsMgr, _pmdObjBase )
      ON_MSG ( MSG_CAT_REG_RES, _onCatRegisterRes )
      ON_MSG ( MSG_CAT_QUERY_TASK_RSP, _onCatQueryTaskRes )
   END_OBJ_MSG_MAP()

   _clsMgr::_clsMgr ()
   :_shdMsgHandlerObj ( &_shardSessionMgr ),
    _replMsgHandlerObj ( &_replSessionMgr ),
    _shdTimerHandler ( &_shardSessionMgr ),
    _replTimerHandler ( &_replSessionMgr ),
    _replNetRtAgent ( &_replMsgHandlerObj ),
    _shardNetRtAgent ( &_shdMsgHandlerObj ),
    _shdObj ( &_shardNetRtAgent ),
    _replObj ( &_replNetRtAgent ),
    _shardSessionMgr( this ),
    _replSessionMgr( this ),
    _shardServiceID ( MSG_ROUTE_SHARD_SERVCIE ),
    _replServiceID ( MSG_ROUTE_REPL_SERVICE ),
    _taskMgr( 0x7FFFFFFF ),
    _taskID ( 0 ),
    _regTimerID ( CLS_INVALID_TIMERID ),
    _oneSecTimerID ( CLS_INVALID_TIMERID )
   {
      _replServiceName[0] = 0 ;
      _shdServiceName[0]  = 0 ;
      _selfNodeID.value   = MSG_INVALID_ROUTEID ;
   }

   _clsMgr::~_clsMgr ()
   {
      SDB_ASSERT( _vecEventHandler.size() == 0, "Has some handler not unreg" ) ;
   }

   SDB_CB_TYPE _clsMgr::cbType () const
   {
      return SDB_CB_CLS ;
   }

   const CHAR* _clsMgr::cbName () const
   {
      return "CLSCB" ;
   }

   INT32 _clsMgr::init ()
   {
      INT32 rc = SDB_OK ;
      NodeID nodeID = _selfNodeID ;
      const CHAR* hostName = pmdGetKRCB()->getHostName() ;
      pmdOptionsCB *optCB = pmdGetOptionCB() ;

      ossStrncpy( _shdServiceName, optCB->shardService(),
                  OSS_MAX_SERVICENAME ) ;
      ossStrncpy( _replServiceName, optCB->replService(),
                  OSS_MAX_SERVICENAME ) ;

      INIT_OBJ_GOTO_ERROR ( getShardCB() ) ;
      INIT_OBJ_GOTO_ERROR ( getReplCB() ) ;

      nodeID.columns.serviceID = _replServiceID ;
      _replNetRtAgent.updateRoute( nodeID, hostName, _replServiceName ) ;
      rc = _replNetRtAgent.listen( nodeID ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG ( PDERROR, "Create listen[Hostname:%s, ServiceName:%s] failed",
                  hostName, _replServiceName ) ;
         goto error ;
      }
      PD_LOG ( PDEVENT, "Create replicate group listen[ServiceName:%s] succeed",
               _replServiceName ) ;

      nodeID.columns.serviceID = _shardServiceID ;
      _shardNetRtAgent.updateRoute( nodeID, hostName, _shdServiceName ) ;
      rc = _shardNetRtAgent.listen( nodeID ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG ( PDERROR, "Create listen[Hostname:%s, ServiceName:%s] failed",
                  hostName, _shdServiceName ) ;
         goto error ;
      }
      PD_LOG ( PDEVENT, "Create sharding listen[ServiceName:%s] succeed",
               _shdServiceName ) ;

      rc = _shardSessionMgr.init( &_shardNetRtAgent, &_shdTimerHandler,
                                  60 * OSS_ONE_SEC ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to init shard session manager, rc: %d",
                   rc ) ;

      rc = _replSessionMgr.init( &_replNetRtAgent, &_replTimerHandler,
                                 OSS_ONE_SEC ) ;
      PD_RC_CHECK( rc, PDERROR, "Failed to init repl session manager, rc: %d",
                   rc ) ;

      pmdGetKRCB()->setBusinessOK( FALSE ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSMGR_ACTIVE, "_clsMgr::active" )
   INT32 _clsMgr::active ()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSMGR_ACTIVE ) ;

      _attachEvent.reset() ;
      rc = _startEDU ( EDU_TYPE_CLUSTER, PMD_EDU_UNKNOW,
                       (_pmdObjBase*)this, TRUE ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _attachEvent.wait( CLS_WAIT_CB_ATTACH_TIMEOUT ) ;
      PD_RC_CHECK( rc, PDERROR, "Wait cluster edu attach failed, rc: %d", rc ) ;

      _attachEvent.reset() ;
      rc = _startEDU ( EDU_TYPE_CLUSTERSHARD, PMD_EDU_UNKNOW,
                       (_pmdObjBase*)getShardCB(), TRUE ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _attachEvent.wait( CLS_WAIT_CB_ATTACH_TIMEOUT ) ;
      PD_RC_CHECK( rc, PDERROR, "Wait cluster-shard attach failed, rc: %d",
                   rc ) ;

      rc = _startEDU( EDU_TYPE_CLSLOGNTY, PMD_EDU_UNKNOW,
                      (_pmdObjBase*)getReplCB(), TRUE ) ;
      if ( rc )
      {
         goto error ;
      }

      rc = _startEDU ( EDU_TYPE_SHARDR, PMD_EDU_RUNNING,
                       (netRouteAgent*)getShardRouteAgent(), TRUE ) ;
      if ( rc )
      {
         goto error ;
      }
      rc = _startEDU ( EDU_TYPE_REPR, PMD_EDU_RUNNING,
                       (netRouteAgent*)getReplRouteAgent(), TRUE ) ;
      if ( rc )
      {
         goto error ;
      }

      _oneSecTimerID = setTimer ( CLS_REPL, OSS_ONE_SEC ) ;

      if ( CLS_INVALID_TIMERID == _oneSecTimerID )
      {
         PD_LOG ( PDERROR, "Register repl/shard/one seccond timer failed" ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      _regTimerID = setTimer( CLS_SHARD, OSS_ONE_SEC ) ;

      if ( CLS_INVALID_TIMERID == _regTimerID )
      {
         PD_LOG ( PDERROR, "Register timer failed" ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      _sendRegisterMsg () ;

   done:
      PD_TRACE_EXITRC ( SDB__CLSMGR_ACTIVE, rc );
      return rc ;
   error:
      goto done ;
   }

   INT32 _clsMgr::deactive ()
   {
      _replNetRtAgent.closeListen() ;
      _shardNetRtAgent.closeListen() ;

      _replObj.deactive() ;
      _shdObj.deactive() ;

      _replNetRtAgent.stop() ;
      _shardNetRtAgent.stop() ;

      _shardSessionMgr.setForced() ;
      _replSessionMgr.setForced() ;

      return SDB_OK ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSMGR_FINAL, "_clsMgr::fini" )
   INT32 _clsMgr::fini ()
   {
      PD_TRACE_ENTRY ( SDB__CLSMGR_FINAL ) ;

      _shardSessionMgr.fini() ;
      _replSessionMgr.fini() ;

      _shdObj.final () ;
      _replObj.final () ;

      PD_TRACE_EXIT ( SDB__CLSMGR_FINAL );
      return SDB_OK ;
   }

   void _clsMgr::onConfigChange ()
   {
      _shdObj.onConfigChange() ;
      _replObj.onConfigChange() ;
   }

   void* _clsMgr::queryInterface( SDB_INTERFACE_TYPE type )
   {
      if ( SDB_IF_EVT_HOLDER == type )
      {
         return (void*)static_cast< IEventHolder* >( this ) ;
      }
      return IControlBlock::queryInterface( type ) ;
   }

   void _clsMgr::attachCB ( pmdEDUCB *pMainCB )
   {
      if ( EDU_TYPE_CLUSTER == pMainCB->getType() )
      {
         _shdMsgHandlerObj.attach ( pMainCB ) ;
         _replMsgHandlerObj.attach ( pMainCB ) ;

         _shdTimerHandler.attach ( pMainCB ) ;
         _replTimerHandler.attach ( pMainCB ) ;
      }
      else if ( EDU_TYPE_CLUSTERSHARD == pMainCB->getType() )
      {
         _shdMsgHandlerObj.attachShardCB( pMainCB ) ;
      }

      _attachEvent.signalAll() ;
   }

   void _clsMgr::detachCB( pmdEDUCB *pMainCB )
   {
      if ( EDU_TYPE_CLUSTER == pMainCB->getType() )
      {
         _shdMsgHandlerObj.detach() ;
         _replMsgHandlerObj.detach () ;

         _shdTimerHandler.detach () ;
         _replTimerHandler.detach () ;
      }
      else if ( EDU_TYPE_CLUSTERSHARD == pMainCB->getType() )
      {
         _shdMsgHandlerObj.detachShardCB() ;
      }
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSMGR__STRATEDU, "_clsMgr::_startEDU" )
   INT32 _clsMgr::_startEDU ( INT32 type, EDU_STATUS waitStatus,
                              void *agrs, BOOLEAN regSys )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSMGR__STRATEDU );
      EDUID eduID = PMD_INVALID_EDUID ;
      pmdKRCB *pKRCB = pmdGetKRCB () ;
      pmdEDUMgr *pEDUMgr = pKRCB->getEDUMgr () ;

      rc = pEDUMgr->startEDU( (EDU_TYPES)type, (void *)agrs, &eduID ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG ( PDERROR, "Failed to create EDU[type:%d(%s)], rc = %d",
                  type, getEDUName( (EDU_TYPES)type ), rc );
         goto error ;
      }

      if ( regSys )
      {
         pEDUMgr->regSystemEDU( (EDU_TYPES)type, eduID ) ;
      }

      if ( PMD_EDU_UNKNOW != waitStatus )
      {
         rc = pEDUMgr->waitUntil( (EDU_TYPES)type, waitStatus ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "Failed to wait EDU[type:%d(%s)] to "
                    "status[%d(%s)], rc: %d", type,
                    getEDUName( (EDU_TYPES)type ), waitStatus,
                    getEDUStatusDesp( waitStatus ), rc ) ;
            goto error ;
         }
      }

   done:
      PD_TRACE_EXITRC ( SDB__CLSMGR__STRATEDU, rc );
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSMGR__ONPRMCHG, "_clsMgr::ntyPrimaryChange" )
   void _clsMgr::ntyPrimaryChange( BOOLEAN primary,
                                   SDB_EVENT_OCCUR_TYPE type )
   {
      PD_TRACE_ENTRY ( SDB__CLSMGR__ONPRMCHG );

      if ( SDB_EVT_OCCUR_AFTER == type )
      {
         PD_LOG ( PDEVENT, "Node change to [%s]",
                  primary ? "Primary" : "Secondary" ) ;
      }

      if ( !pmdGetStartup().isOK() )
      {
         return ;
      }

      if ( primary && SDB_EVT_OCCUR_BEFORE == type )
      {
         sdbGetDPSCB()->incVersion() ;
      }
      else if ( !primary && SDB_EVT_OCCUR_BEFORE == type )
      {
         pmdGetKRCB()->getEDUMgr()->interruptWritingEDUS() ;
      }

      getShardCB()->ntyPrimaryChange( primary, type ) ;
      getReplCB()->ntyPrimaryChange( primary, type ) ;

      if ( SDB_EVT_OCCUR_AFTER == type )
      {
         if ( primary )
         {
            BSONObj match = BSON ( CAT_TARGETID_NAME <<
                                   _selfNodeID.columns.groupID ) ;
            startTaskCheck( match ) ;
         }
         else
         {
            ossScopedLock lock ( &_clsLatch, EXCLUSIVE ) ;
            _mapTaskQuery.clear () ;
         }
      }

      _callPrimaryChangeHandler( primary, type ) ;

      PD_TRACE_EXIT ( SDB__CLSMGR__ONPRMCHG );
   }

   const CHAR *_clsMgr::getShardServiceName () const
   {
      return _shdServiceName ;
   }
   const CHAR *_clsMgr::getReplServiceName () const
   {
      return _replServiceName ;
   }
   NodeID _clsMgr::getNodeID () const
   {
      return _selfNodeID ;
   }
   UINT16 _clsMgr::getShardServiceID () const
   {
      return _shardServiceID ;
   }
   UINT16 _clsMgr::getReplServiceID () const
   {
      return _replServiceID ;
   }

   _netRouteAgent *_clsMgr::getShardRouteAgent ()
   {
      return &_shardNetRtAgent ;
   }
   _netRouteAgent *_clsMgr::getReplRouteAgent ()
   {
      return &_replNetRtAgent ;
   }
   shardCB *_clsMgr::getShardCB ()
   {
      return &_shdObj ;
   }
   replCB *_clsMgr::getReplCB ()
   {
      return &_replObj ;
   }
   catAgent *_clsMgr::getCatAgent ()
   {
      return _shdObj.getCataAgent() ;
   }
   nodeMgrAgent* _clsMgr::getNodeMgrAgent ()
   {
      return _shdObj.getNodeMgrAgent() ;
   }
   pmdAsyncMsgHandler* _clsMgr::getShardMsgHandle()
   {
      return &_shdMsgHandlerObj ;
   }
   _clsTaskMgr* _clsMgr::getTaskMgr()
   {
      return &_taskMgr ;
   }
   BOOLEAN _clsMgr::isPrimary ()
   {
      return _replObj.primaryIsMe () ;
   }
   BOOLEAN _clsMgr::isFullSync ()
   {
      return _replObj.isFullSync() ;
   }
   INT32 _clsMgr::clearAllData ()
   {
      return _shdObj.clearAllData () ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSMGR_INVDATACAT, "_clsMgr::invalidateCata" )
   INT32 _clsMgr::invalidateCata( const CHAR * name )
   {
      INT32 rc = SDB_CLS_NOT_PRIMARY ;
      PD_TRACE_ENTRY ( SDB__CLSMGR_INVDATACAT );

      if ( isPrimary() )
      {
         SDB_DPSCB *dpsCB = pmdGetKRCB()->getDPSCB() ;
         dpsMergeInfo info ;
         info.setInfoEx( ~0, ~0, DMS_INVALID_EXTENT, NULL ) ;
         dpsLogRecord &record = info.getMergeBlock().record() ;
         rc = dpsInvalidCata2Record( name, record ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to build invalid-cata log:%d",rc ) ;
            goto error ;
         }
         rc = dpsCB->prepare(info ) ;
         if ( SDB_OK == rc )
         {
            dpsCB->writeData( info ) ;
         }
      }

   done:
      PD_TRACE_EXITRC ( SDB__CLSMGR_INVDATACAT, rc );
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSMGR_STARTINSN, "_clsMgr::startInnerSession" )
   INT32 _clsMgr::startInnerSession ( INT32 type, INT32 innerTID, void *data )
   {
      PD_TRACE_ENTRY ( SDB__CLSMGR_STARTINSN );
      ossScopedLock lock ( &_clsLatch, EXCLUSIVE ) ;

      _innerSessionInfo info ;
      info.type = type ;
      info.startType = PMD_SESSION_ACTIVE ;
      info.innerTid = innerTID ;
      info.data = data ;
      info.sessionID = ossPack32To64 ( _selfNodeID.columns.nodeID, innerTID ) ;

      _vecInnerSessionParam.push_back ( info ) ;

      PD_TRACE_EXIT ( SDB__CLSMGR_STARTINSN );
      return SDB_OK ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSMGR_STARTTSKCHK, "_clsMgr::startTaskCheck" )
   INT32 _clsMgr::startTaskCheck ( const BSONObj & match )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSMGR_STARTTSKCHK );
      if ( !isPrimary() )
      {
         rc = SDB_CLS_NOT_PRIMARY ;
      }
      else
      {
         ossScopedLock lock ( &_clsLatch, EXCLUSIVE ) ;
         _mapTaskQuery[++_taskID] = match.copy() ;
      }
      PD_TRACE_EXIT ( SDB__CLSMGR_STARTTSKCHK );
      return rc ;
   }

   INT32 _clsMgr::stopTask( UINT64 taskID )
   {
      ossScopedLock lock ( &_clsLatch, SHARED ) ;
      map< UINT64, UINT64 >::iterator it = _mapTaskID.find( taskID ) ;
      if ( it != _mapTaskID.end() )
      {
         _taskMgr.stopTask( it->second ) ;
      }
      return SDB_OK ;
   }

   INT32 _clsMgr::removeTask( UINT64 taskID )
   {
      ossScopedLock lock ( &_clsLatch, EXCLUSIVE ) ;
      map< UINT64, UINT64 >::iterator it = _mapTaskID.find( taskID ) ;
      if ( it != _mapTaskID.end() )
      {
         _mapTaskID.erase( it ) ;
      }
      return SDB_OK ;
   }

   INT32 _clsMgr::regEventHandler( IEventHander * pHandler )
   {
      if ( !pHandler ) return SDB_INVALIDARG ;

      ossScopedLock lock ( &_handlerLatch, EXCLUSIVE ) ;
      for ( UINT32 i = 0 ; i < _vecEventHandler.size() ; ++i )
      {
         if ( _vecEventHandler[ i ] == pHandler )
         {
            return SDB_SYS ;
         }
      }
      _vecEventHandler.push_back( pHandler ) ;
      return SDB_OK ;
   }

   void _clsMgr::unregEventHandler( IEventHander * pHandler )
   {
      if ( !pHandler ) return ;

      ossScopedLock lock ( &_handlerLatch, EXCLUSIVE ) ;
      VEC_EVENTHANDLER::iterator it ;
      for ( it = _vecEventHandler.begin() ;
            it != _vecEventHandler.end() ;
            ++it )
      {
         if ( *it == pHandler )
         {
            _vecEventHandler.erase( it ) ;
            break ;
         }
      }
   }

   void _clsMgr::_callRegisterEventHandler()
   {
      IEventHander *pHandler = NULL ;
      ossScopedLock lock ( &_handlerLatch, SHARED ) ;
      for ( UINT32 i = 0 ; i < _vecEventHandler.size() ; ++i )
      {
         pHandler = _vecEventHandler[ i ] ;
         if ( pHandler->getMask() & EVENT_MASK_ON_REGISTERED )
         {
            pHandler->onRegistered( _selfNodeID ) ;
         }
      }
   }

   void _clsMgr::_callPrimaryChangeHandler( BOOLEAN primary,
                                            SDB_EVENT_OCCUR_TYPE type )
   {
      IEventHander *pHandler = NULL ;
      ossScopedLock lock ( &_handlerLatch, SHARED ) ;
      for ( UINT32 i = 0 ; i < _vecEventHandler.size() ; ++i )
      {
         pHandler = _vecEventHandler[ i ] ;
         if ( pHandler->getMask() & EVENT_MASK_ON_PRIMARYCHG )
         {
            pHandler->onPrimaryChange( primary, type ) ;
         }
      }
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSMGR__DFTMSGFUNC, "_clsMgr::_defaultMsgFunc" )
   INT32 _clsMgr::_defaultMsgFunc( NET_HANDLE handle, MsgHeader * msg )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSMGR__DFTMSGFUNC );
      INT32 type = (INT32) msg->TID ;
      UINT32 opCode = (UINT32)(msg->opCode) ;
      msg->TID = 0 ;

      if ( CLS_REPL == type || MSG_CAT_GRP_RES == opCode
         || MSG_CAT_PAIMARY_CHANGE_RES == opCode
         || MSG_CLS_GINFO_UPDATED == opCode )
      {
         rc = _replObj.dispatchMsg( handle, msg ) ;
      }
      else
      {
         rc = _shdObj.dispatchMsg ( handle, msg ) ;
      }
      PD_TRACE_EXITRC ( SDB__CLSMGR__DFTMSGFUNC, rc );
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSMGR_ONTMR, "_clsMgr::onTimer" )
   void _clsMgr::onTimer ( UINT64 timerID, UINT32 interval )
   {
      PD_TRACE_ENTRY ( SDB__CLSMGR_ONTMR );
      if ( timerID == _regTimerID )
      {
         _sendRegisterMsg () ;
      }
      else if ( timerID == _oneSecTimerID )
      {
         _shardSessionMgr.onTimer( interval ) ;
         _replSessionMgr.onTimer( interval ) ;

         _prepareTask () ;

         if ( _taskMgr.taskCount() > 0 &&
              !_shardSessionMgr.isUnShardTimerStarted() )
         {
            _shardSessionMgr.startUnShardTimer( OSS_ONE_SEC ) ;
         }
         else if ( _shardSessionMgr.isUnShardTimerStarted() &&
                   0 == _taskMgr.taskCount() )
         {
            _shardSessionMgr.stopUnShardTimer() ;
         }
      }
      else
      {
         UINT32 type = 0 ;
         UINT32 netTimerID = 0 ;
         ossUnpack32From64 ( timerID, type, netTimerID ) ;
         _pmdObjBase *pSubObj = &_shdObj ;
         if ( CLS_REPL == (INT32)type )
         {
            pSubObj = &_replObj ;
         }

         pSubObj->onTimer ( timerID, interval ) ;
      }
      PD_TRACE_EXIT ( SDB__CLSMGR_ONTMR );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSMGR__STARTINSN, "_clsMgr::_startInnerSession" )
   INT32 _clsMgr::_startInnerSession ( INT32 type,
                                       pmdAsycSessionMgr *pSessionMgr )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSMGR__STARTINSN );
      _pmdAsyncSession *pSession = NULL ;
      ossScopedLock lock ( &_clsLatch, EXCLUSIVE ) ;

      VECINNERPARAM::iterator it = _vecInnerSessionParam.begin() ;
      while ( it != _vecInnerSessionParam.end() )
      {
         _innerSessionInfo &info = *it ;
         if ( info.type != type || pSessionMgr->getSession( info.sessionID,
                                                            info.startType,
                                                            NET_INVALID_HANDLE,
                                                            FALSE, 0, NULL ) )
         {
            ++it ;
            continue ;
         }
         pSession = pSessionMgr->getSession ( info.sessionID, info.startType,
                                              NET_INVALID_HANDLE, TRUE, 0,
                                              info.data ) ;
         if ( pSession )
         {
            PD_LOG ( PDEVENT, "Create inner session[%s] succeed",
                     pSession->sessionName() ) ;
            it = _vecInnerSessionParam.erase ( it ) ;
            continue ;
         }
         PD_LOG ( PDERROR, "Create inner session[TID:%d] failed",
                  info.innerTid ) ;
         rc = SDB_SYS ;

         ++it ;
      }

      PD_TRACE_EXITRC ( SDB__CLSMGR__STARTINSN, rc ) ;
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSMGR__PREPTASK, "_clsMgr::_prepareTask" )
   INT32 _clsMgr::_prepareTask ()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSMGR__PREPTASK );
      ossScopedLock lock ( &_clsLatch, SHARED ) ;
      MAPTASKQUERY::iterator it = _mapTaskQuery.begin () ;
      while ( it != _mapTaskQuery.end() )
      {
         rc = _sendQueryTaskReq ( it->first, "CAT", &(it->second) ) ;
         if ( SDB_OK != rc )
         {
            break ;
         }
         ++it ;
      }
      PD_TRACE_EXITRC ( SDB__CLSMGR__PREPTASK, rc );
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSMGR__ADDTSKINSN, "_clsMgr::_addTaskInnerSession" )
   INT32 _clsMgr::_addTaskInnerSession ( const CHAR * objdata )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSMGR__ADDTSKINSN );
      INT32 jobType = CLS_TASK_UNKNOW ;
      _clsSplitTask *pTask = NULL ;
      UINT32 tid = 0 ;
      INT32 type = CLS_SHARD ;
      UINT64 taskID = CLS_INVALID_TASKID ;

      try
      {
         BSONObj resultObj ( objdata ) ;
         BSONElement ele = resultObj.getField( CAT_TASKTYPE_NAME ) ;
         PD_CHECK ( ele.type() == NumberInt, SDB_INVALIDARG, error, PDERROR,
                    "Field[%s] invalid in task[%s]", CAT_TASKTYPE_NAME,
                    resultObj.toString().c_str() ) ;
         jobType = ele.numberInt () ;
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "addTaskInnerSession exception: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      switch ( jobType )
      {
         case CLS_TASK_SPLIT :
            taskID = _taskMgr.getTaskID() ;
            pTask = SDB_OSS_NEW _clsSplitTask ( taskID ) ;
            type = CLS_SHARD ;
            break ;
         default :
            PD_LOG ( PDERROR, "Unknow job type[%d]", jobType ) ;
            rc = SDB_INVALIDARG ;
            break ;
      }

      if ( SDB_OK == rc && !pTask )
      {
         PD_LOG ( PDERROR, "Failed to alloc memory for task[type:%d]",
                  jobType ) ;
         rc = SDB_OOM ;
         goto error ;
      }
      else if ( !pTask )
      {
         goto error ;
      }

      rc = pTask->init( objdata ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG ( PDERROR, "Init task failed[rc:%d]", rc ) ;
         goto error ;
      }

      rc = _taskMgr.addTask( pTask, taskID ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG ( PDERROR, "Failed to add task, rc = %d", rc ) ;
         pTask = NULL ;
         goto error ;
      }

      _clsLatch.get() ;
      _mapTaskID[ pTask->taskID() ] = taskID ;
      _clsLatch.release() ;

      tid = (UINT32)taskID ;
      rc = startInnerSession ( type, tid, (void *)pTask ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to start inner session, rc = %d",
                  rc ) ;
         pTask = NULL ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC ( SDB__CLSMGR__ADDTSKINSN, rc );
      return rc ;
   error:
      if ( pTask )
      {
         SDB_OSS_DEL pTask ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSMGR_SETTMR, "_clsMgr::setTimer" )
   UINT64 _clsMgr::setTimer ( CLS_MEMBER_TYPE type, UINT32 milliSec )
   {
      UINT64 rc;
      PD_TRACE_ENTRY ( SDB__CLSMGR_SETTMR );
      UINT32 timeID = 0 ;
      _netTimeoutHandler * pHandler = &_shdTimerHandler ;
      _netRouteAgent * pRtAgent = &_shardNetRtAgent ;

      if ( CLS_REPL == type )
      {
         pHandler = &_replTimerHandler ;
         pRtAgent = &_replNetRtAgent ;
      }

      if ( pRtAgent->addTimer( milliSec, pHandler, timeID ) == SDB_OK )
      {
         rc = ossPack32To64( (UINT32)type, timeID ) ;
      }
      else
      {
         rc = CLS_INVALID_TIMERID ;
      }
      PD_TRACE1( SDB__CLSMGR_SETTMR, PD_PACK_ULONG(rc) );
      PD_TRACE_EXIT ( SDB__CLSMGR_SETTMR );
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSMGR_KILLTMR, "_clsMgr::killTimer" )
   void _clsMgr::killTimer( UINT64 timerID )
   {
      PD_TRACE_ENTRY ( SDB__CLSMGR_KILLTMR );
      UINT32 type = 0 ;
      UINT32 netTimerID = 0 ;

      ossUnpack32From64 ( timerID, type, netTimerID ) ;

      _netRouteAgent * pRtAgent = &_shardNetRtAgent ;

      if ( CLS_REPL == (INT32)type )
      {
         pRtAgent = &_replNetRtAgent ;
      }

      pRtAgent->removeTimer( netTimerID ) ;
      PD_TRACE_EXIT ( SDB__CLSMGR_KILLTMR );
   }

   INT32 _clsMgr::sendToCatlog ( MsgHeader * msg )
   {
      return _shdObj.sendToCatlog ( msg ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSMGR__SNDREGMSG, "_clsMgr::_sendRegisterMsg" )
   INT32 _clsMgr::_sendRegisterMsg ()
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSMGR__SNDREGMSG );
      pmdKRCB *pKRCB = pmdGetKRCB () ;
      BSONObjBuilder bsonBuilder ;
      const CHAR* hostName = pmdGetKRCB()->getHostName() ;

      bsonBuilder.append ( CAT_TYPE_FIELD_NAME,  (INT32)(pKRCB->getDBRole()) ) ;
      bsonBuilder.append ( CAT_HOST_FIELD_NAME, hostName ) ;

      BSONArrayBuilder arrayBuilder ;
      BSONObjBuilder subBuilderRepl, subBuilderShd ;

      subBuilderRepl.append ( CAT_SERVICE_TYPE_FIELD_NAME ,
                              (INT32)_replServiceID ) ;
      subBuilderRepl.append ( CAT_SERVICE_NAME_FIELD_NAME,
                              _replServiceName ) ;
      arrayBuilder.append( subBuilderRepl.obj() ) ;

      subBuilderShd.append ( CAT_SERVICE_TYPE_FIELD_NAME ,
                             (INT32)_shardServiceID) ;
      subBuilderShd.append ( CAT_SERVICE_NAME_FIELD_NAME,
                             _shdServiceName) ;
      arrayBuilder.append( subBuilderShd.obj() ) ;

      bsonBuilder.appendArray ( CAT_SERVICE_FIELD_NAME, arrayBuilder.arr() ) ;

      ossIPInfo ipInfo ;
      if ( ipInfo.getIPNum() > 0 )
      {
         BSONArrayBuilder ipArray ;

         ossIP* ip = ipInfo.getIPs() ;
         for ( INT32 i = ipInfo.getIPNum(); i > 0; i-- )
         {
            if (0 != ossStrncmp( ip->ipAddr, OSS_LOOPBACK_IP,
                                 ossStrlen(OSS_LOOPBACK_IP)) )
            {
               ipArray.append( ip->ipAddr ) ;
            }
            ip++ ;
         }

         ipArray.append( OSS_LOOPBACK_IP ) ;
         ipArray.append( OSS_LOCALHOST ) ;

         bsonBuilder.appendArray ( CAT_IP_FIELD_NAME, ipArray.arr() ) ;
      }

      BSONObj regObj = bsonBuilder.obj () ;
      UINT32 length = regObj.objsize () + sizeof ( MsgCatRegisterReq ) ;
      CHAR * buff = (CHAR *)SDB_OSS_MALLOC ( length ) ;
      MsgCatRegisterReq *pReq = NULL ;

      if ( buff == NULL )
      {
         PD_LOG ( PDERROR, "Failed to allocate memroy for register req" ) ;
         rc = SDB_OOM ;
         goto error ;
      }

      pReq = (MsgCatRegisterReq*)buff ;
      pReq->header.messageLength = length ;
      pReq->header.opCode = MSG_CAT_REG_REQ ;
      pReq->header.requestID = 0 ;
      pReq->header.TID = 0 ;
      pReq->header.routeID.value = 0 ;
      ossMemcpy( pReq->data, regObj.objdata(), regObj.objsize() ) ;

      rc = sendToCatlog( (MsgHeader *) pReq ) ;
      PD_LOG ( PDDEBUG, "Send node register[rc: %d]", rc ) ;

   done:
      if ( buff )
      {
         SDB_OSS_FREE ( buff ) ;
         buff = NULL ;
      }
      PD_TRACE_EXITRC ( SDB__CLSMGR__SNDREGMSG, rc );
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSMGR__SNDQTSKREQ, "_clsMgr::_sendQueryTaskReq" )
   INT32 _clsMgr::_sendQueryTaskReq ( UINT64 requestID, const CHAR * clFullName,
                                      const BSONObj* match )
   {
      PD_TRACE_ENTRY ( SDB__CLSMGR__SNDQTSKREQ );
      CHAR *pBuff = NULL ;
      INT32 buffSize = 0 ;
      MsgHeader *msg = NULL ;
      INT32 rc = SDB_OK ;

      rc = msgBuildQueryMsg ( &pBuff, &buffSize, clFullName, 0, requestID,
                              0, -1, match, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }
      msg = ( MsgHeader* )pBuff ;
      msg->opCode = MSG_CAT_QUERY_TASK_REQ ;
      msg->TID = 0 ;
      msg->routeID.value = 0 ;

      rc = sendToCatlog( msg ) ;
      PD_LOG ( PDDEBUG, "Send MSG_CAT_QUERY_TASK_REQ[%s] to catalog[rc:%d]",
               match->toString().c_str(), rc ) ;
   done:
      if ( pBuff )
      {
         SDB_OSS_FREE ( pBuff ) ;
         pBuff = NULL ;
      }
      PD_TRACE_EXITRC ( SDB__CLSMGR__SNDQTSKREQ, rc );
      return rc ;
   error:
      goto done ;
   }

   INT32 _clsMgr::updateCatGroup ( BOOLEAN unsetPrimary, INT64 millisec )
   {
      return _shdObj.updateCatGroup ( unsetPrimary, millisec ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSMGR__ONCATREGRES, "_clsMgr::_onCatRegisterRes" )
   INT32 _clsMgr::_onCatRegisterRes ( NET_HANDLE handle, MsgHeader* msg )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB__CLSMGR__ONCATREGRES );
      NodeID nodeID ;
      _MsgCatRegisterRsp * res = (_MsgCatRegisterRsp*)msg ;

      if ( _regTimerID == CLS_INVALID_TIMERID )
      {
         goto done ;
      }

      rc = res->header.res ;
      if ( SDB_CLS_NOT_PRIMARY == res->header.res )
      {
         updateCatGroup ( TRUE ) ;
         goto error ;
      }
      else if ( res->header.res != SDB_OK )
      {
         PD_LOG ( PDSEVERE, "Node register failed[Respone:%d]",
                  res->header.res ) ;
         goto error ;
      }

      {
         BSONObj object ( (const char*)(res->data) );
         BSONElement gidEl = object.getField ( CAT_GROUPID_NAME ) ;
         BSONElement nidEl = object.getField ( CAT_NODEID_NAME ) ;

         if ( gidEl.type() != NumberInt || nidEl.type() != NumberInt )
         {
            rc = SDB_SYS ;
            PD_LOG ( PDERROR, "Node register response error" ) ;
            goto error ;
         }

         killTimer ( _regTimerID ) ;
         _regTimerID = CLS_INVALID_TIMERID ;

         _selfNodeID.columns.groupID = (UINT32)gidEl.Int () ;
         _selfNodeID.columns.nodeID = (UINT32)nidEl.Int () ;
         _shdObj.setNodeID( _selfNodeID ) ;
         PD_LOG ( PDEVENT, "Register succeed, groupID:%u, nodeID:%u",
                  _selfNodeID.columns.groupID,
                  _selfNodeID.columns.nodeID ) ;

         BSONElement hostEle = object.getField ( CAT_HOST_FIELD_NAME ) ;
         if ( hostEle.type() == String )
         {
            /*
             * The node can be created by hostname or ip,
             * so the actual 'HostName' maybe current host's name or ip address.
             * Here we ensure the KRCB's HostName is consistent with catalog.
             */
            pmdGetKRCB()->setHostName( hostEle.String().c_str() ) ;
         }
      }

      nodeID.value = _selfNodeID.value ;
      nodeID.columns.serviceID = _replServiceID ;
      _replNetRtAgent.setLocalID ( nodeID ) ;
      nodeID.columns.serviceID = _shardServiceID ;
      _shardNetRtAgent.setLocalID ( nodeID ) ;

      pmdSetNodeID( _selfNodeID ) ;

      _callRegisterEventHandler() ;

      pmdGetKRCB()->setBusinessOK( TRUE ) ;

      if ( SDB_OK != _shdObj.updatePrimary( msg->routeID, TRUE ) )
      {
         _shdObj.updateCatGroup ( FALSE ) ;
      }

      rc = _shdObj.active () ;
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "active shardCB failed[rc:%d]", rc ) ;
         goto error ;
      }

      rc = _replObj.active () ;
      if ( rc != SDB_OK )
      {
         PD_LOG ( PDERROR, "active replCB failed[rc:%d]", rc ) ;
         goto error ;
      }

   done:
      PD_TRACE_EXITRC (SDB__CLSMGR__ONCATREGRES, rc );
      return rc ;
   error:
      if ( res->header.res == SDB_CAT_AUTH_FAILED )
      {
         PD_LOG ( PDSEVERE, "Catlog auth the db node failed, shutdown..." ) ;
         PMD_SHUTDOWN_DB( SDB_CAT_AUTH_FAILED ) ;
      }
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB__CLSMGR__ONCATQTSKRES, "_clsMgr::_onCatQueryTaskRes" )
   INT32 _clsMgr::_onCatQueryTaskRes ( NET_HANDLE handle, MsgHeader * msg )
   {
      PD_TRACE_ENTRY ( SDB__CLSMGR__ONCATQTSKRES );
      MsgCatQueryTaskRes *res = ( MsgCatQueryTaskRes* )msg ;
      PD_LOG ( PDDEBUG, "Recieve catalog query task response[requestID:%lld, "
               "flag: %d]", msg->requestID, res->flags ) ;

      INT32 rc = SDB_OK ;
      INT32 flag = 0 ;
      INT64 contextID = -1 ;
      INT32 startFrom = 0 ;
      INT32 numReturned = 0 ;
      vector<BSONObj> objList ;

      if ( SDB_CLS_NOT_PRIMARY == res->flags )
      {
         updateCatGroup( TRUE ) ;
      }
      else if ( SDB_DMS_EOC == res->flags ||
                SDB_CAT_TASK_NOTFOUND == res->flags )
      {
         _clsLatch.get() ;
         _mapTaskQuery.erase ( msg->requestID ) ;
         _clsLatch.release() ;
         PD_LOG ( PDINFO, "The query task[%lld] has 0 jobs", msg->requestID ) ;
      }
      else if ( SDB_OK != res->flags )
      {
         PD_LOG ( PDERROR, "Query task[%lld] failed[rc=%d]",
                  msg->requestID, res->flags ) ;
         goto error ;
      }
      else
      {
         rc = msgExtractReply ( (CHAR *)msg, &flag, &contextID, &startFrom,
                                &numReturned, objList ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }

         {
            ossScopedLock lock ( &_clsLatch, EXCLUSIVE ) ;
            MAPTASKQUERY::iterator it = _mapTaskQuery.find ( msg->requestID ) ;
            if ( it == _mapTaskQuery.end() )
            {
               PD_LOG ( PDWARNING, "The query task response[%lld] is not exist",
                        msg->requestID ) ;
               rc = SDB_INVALIDARG ;
               goto error ;
            }
            _mapTaskQuery.erase ( it ) ;
         }

         PD_LOG ( PDINFO, "The query task[%lld] has %d jobs", msg->requestID,
                  numReturned ) ;

         {
            UINT32 index = 0 ;
            while ( index < objList.size() )
            {
               _addTaskInnerSession ( objList[index].objdata() ) ;
               ++index ;
            }
         }
      }

   done:
      PD_TRACE_EXITRC ( SDB__CLSMGR__ONCATQTSKRES, rc );
      return rc ;
   error:
      goto done ;
   }

   /*
      get global cls cb
   */
   clsCB* sdbGetClsCB ()
   {
      static clsCB s_clsCB ;
      return &s_clsCB ;
   }
   shardCB* sdbGetShardCB ()
   {
      return sdbGetClsCB()->getShardCB() ;
   }
   replCB* sdbGetReplCB ()
   {
      return sdbGetClsCB()->getReplCB() ;
   }

}

