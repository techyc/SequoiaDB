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

   Source File Name = omagentNodeMgr.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/06/2014  XJH Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef OMAGENT_NODEMGR_HPP__
#define OMAGENT_NODEMGR_HPP__

#include "omagentDef.hpp"
#include "pmdDaemon.hpp"
#include "rtnBackgroundJobBase.hpp"

#include <deque>
#include <string>
#include <map>
using namespace std ;

namespace engine
{

   class _omAgentNodeMgr ;

   #define OM_NODE_LOCK_BUCKET_SIZE          ( 256 )

   /*
      Process Status Define
   */
   enum OMNODE_STATUS
   {
      OMNODE_NORMAL           = 0,
      OMNODE_RUNNING,
      OMNODE_CRASH,
      OMNODE_RESTART,
      OMNODE_REMOVING
   } ;

   /*
      _dbProcessInfo define
   */
   struct _dbProcessInfo
   {
      OSSPID               _pid ;
      OMNODE_STATUS        _status ;
      deque< time_t >      _startTime ;
      INT32                _errNum ;

      _dbProcessInfo()
      {
         _pid        = OSS_INVALID_PID ;
         _status     = OMNODE_NORMAL ;
         _errNum     = 0 ;
      }

      void reset()
      {
         _pid        = OSS_INVALID_PID ;
         _status     = OMNODE_NORMAL ;
         _errNum     = 0 ;
      }
   } ;
   typedef _dbProcessInfo dbProcessInfo ;

   /*
      NODE_START_TYPE define
   */
   enum NODE_START_TYPE
   {
      NODE_START_CLIENT          = 1,
      NODE_START_MONITOR,
      NODE_START_SYSTEM
   } ;

   /*
      _startNodeJob define
   */
   class _startNodeJob : public _rtnBaseJob
   {
      public:
         _startNodeJob( const string &svcname, NODE_START_TYPE startType,
                        _omAgentNodeMgr *pNodeMgr ) ;
         virtual ~_startNodeJob() ;

         const string& getSvcName() const { return _svcName ; }

      public:
         virtual RTN_JOB_TYPE type () const ;
         virtual const CHAR* name () const ;
         virtual BOOLEAN muteXOn ( const _rtnBaseJob *pOther ) ;
         virtual INT32 doit () ;

      private:
         string               _svcName ;
         NODE_START_TYPE      _startType ;
         _omAgentNodeMgr      *_pNodeMgr ;
         string               _jobName ;

   } ;
   typedef _startNodeJob startNodeJob ;

   INT32 startStartNodeJOb ( const string &svcname,
                             NODE_START_TYPE startType,
                             _omAgentNodeMgr *pNodeMgr,
                             EDUID *pEDUID = NULL,
                             BOOLEAN returnResult = FALSE ) ;

   /*
      _cmSyncJob define
   */
   class _cmSyncJob : public _rtnBaseJob
   {
      public:
         _cmSyncJob( _omAgentNodeMgr *pNodeMgr ) ;
         virtual ~_cmSyncJob() ;

      public:
         virtual RTN_JOB_TYPE type () const ;
         virtual const CHAR* name () const ;
         virtual BOOLEAN muteXOn ( const _rtnBaseJob *pOther ) ;
         virtual INT32 doit () ;

      private:
         _omAgentNodeMgr      *_pNodeMgr ;
   } ;
   typedef _cmSyncJob cmSyncJob ;

   INT32 startCMSyncJob( _omAgentNodeMgr *pNodeMgr,
                         EDUID *pEDUID = NULL,
                         BOOLEAN returnResult = FALSE ) ;

   /*
      _omAgentNodeMgr define
   */
   class _omAgentNodeMgr : public cCMService
   {
      typedef map< string, dbProcessInfo >         MAP_DB_PROCESS ;
      typedef MAP_DB_PROCESS::iterator             MAP_DB_PROCESS_IT ;

      public:
         _omAgentNodeMgr() ;
         virtual ~_omAgentNodeMgr() ;

         INT32    init() ;
         INT32    active() ;
         INT32    fini() ;

         /*
            For each not running node, start a job to start the node
         */
         INT32    startAllNodes( NODE_START_TYPE startType ) ;
         /*
            Stop all Sequoiadb Nodes
         */
         INT32    stopAllNodes() ;
         /*
            Check the running node, if crash, start a job to start the node
         */
         void     monitorNodes() ;
         /*
            Clean the status = OMNODE_REMOVING nodes
         */
         void     cleanDeadNodes() ;
         /*
            Watch the nodes that create by user created manually
         */
         void     watchManualNodes() ;

         INT32    addANode( const CHAR *arg1, const CHAR *arg2,
                            string *omsvc = NULL ) ;
         INT32    rmANode( const CHAR *arg1, const CHAR *arg2,
                           const CHAR *roleStr = NULL,
                           string *omsvc = NULL ) ;
         INT32    mdyANode( const CHAR *arg1 ) ;
         INT32    startANode( const CHAR *arg1 ) ;
         INT32    stopANode( const CHAR *arg1 ) ;

      public:

         INT32    addNodeProcessInfo( const string &svcname ) ;
         INT32    delNodeProcessInfo( const string &svcname ) ;
         dbProcessInfo* getNodeProcessInfo( const string &svcname ) ;

         INT32    startANode( const CHAR *svcname, NODE_START_TYPE type,
                              BOOLEAN needLock ) ;
         INT32    stopANode( const CHAR *svcname, NODE_START_TYPE type,
                             BOOLEAN needLock ) ;

      protected:
         void     lockBucket( const string &svcname ) ;
         void     releaseBucket( const string &svcname ) ;
         ossSpinXLatch* getBucket( const string &svcname ) ;

      protected:
         INT32    _addANode( const CHAR *arg1, const CHAR *arg2,
                             BOOLEAN needLock, BOOLEAN isModify,
                             string *omsvc = NULL ) ;

         const CHAR* _getSvcNameFromArg( const CHAR *arg ) ;

         void     _checkNodeByStartupFile( const CHAR *pSvcName,
                                           dbProcessInfo *pInfo ) ;

      private:
         MAP_DB_PROCESS                _mapDBProcess ;
         ossSpinSLatch                 _mapLatch ;
         ossSpinXLatch                 _lockBucket[ OM_NODE_LOCK_BUCKET_SIZE ] ;

   } ;
   typedef _omAgentNodeMgr omAgentNodeMgr ;

}

#endif // OMAGENT_NODEMGR_HPP__

