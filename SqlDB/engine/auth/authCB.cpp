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

   Source File Name = authCB.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/12/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "authCB.hpp"
#include "authDef.hpp"
#include "pmd.hpp"
#include "rtn.hpp"
#include "authTrace.hpp"
#include "pmdCB.hpp"
#include "catCommon.hpp"

using namespace bson ;

namespace engine
{
   _authCB::_authCB():_needAuth( FALSE )
   {

   }

   _authCB::~_authCB()
   {
   }

   INT32 _authCB::init ()
   {
      pmdEDUCB *cb = pmdGetThreadEDUCB() ;
      return _initAuthentication( cb ) ;
   }

   INT32 _authCB::active ()
   {
      pmdEDUCB *cb = pmdGetThreadEDUCB() ;
      INT32 rc = checkNeedAuth( cb, TRUE ) ;
      if ( rc )
      {
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _authCB::deactive ()
   {
      return SDB_OK ;
   }

   INT32 _authCB::fini ()
   {
      return SDB_OK ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_AUTHCB_AUTHENTICATE, "_authCB::authenticate" )
   INT32 _authCB::authenticate( BSONObj &obj, _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      BSONObj hint ;
      BSONObj selector ;
      BSONObj order ;
      SDB_DMSCB *dmsCB = pmdGetKRCB()->getDMSCB() ;
      SDB_RTNCB *rtnCB = pmdGetKRCB()->getRTNCB() ;
      SINT64 contextID = -1 ;
      rtnContextBuf buffObj ;

      PD_TRACE_ENTRY ( SDB_AUTHCB_AUTHENTICATE ) ;
      try
      {
         hint = BSON( "" << AUTH_USR_INDEX_NAME ) ;
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDWARNING, "Failed to initialize hint: %s",
                  e.what() ) ;
      }

      PD_LOG( PDDEBUG, "get authentication msg:[%s]",
              obj.toString().c_str()) ;

      if ( SDB_OK != _valid( obj, FALSE ) )
      {
         rc = SDB_INVALIDARG;
         goto error ;
      }

      rc = rtnQuery( AUTH_USR_COLLECTION,
                     selector, obj, order,
                     hint, 0, cb, 0, -1, dmsCB,
                     rtnCB, contextID ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to query:%d",rc ) ;
         goto error ;
      }

      rc = rtnGetMore( contextID, -1, buffObj, cb, rtnCB ) ;
      if ( SDB_OK != rc && SDB_DMS_EOC != rc)
      {
         PD_LOG( PDERROR, "failed to getmore:%d",rc ) ;
         rc = SDB_AUTH_AUTHORITY_FORBIDDEN ;
         goto error ;
      }
      else if ( SDB_DMS_EOC == rc )
      {
         rc = SDB_AUTH_AUTHORITY_FORBIDDEN ;
         goto error ;
      }
      else if ( 0 == buffObj.recordNum() )
      {
         rc = SDB_AUTH_AUTHORITY_FORBIDDEN ;
         goto error ;
      }
      else if ( 1 == buffObj.recordNum() )
      {
         rc = SDB_OK ;
      }
      else
      {
         PD_LOG( PDERROR, "get more than one record, impossible" ) ;
         rc = SDB_SYS ;
         SDB_ASSERT( FALSE, "impossible" ) ;
      }

   done:
      if ( -1 != contextID )
      {
         rtnKillContexts( 1, &contextID, cb, rtnCB ) ;
      }
      PD_TRACE_EXITRC ( SDB_AUTHCB_AUTHENTICATE, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_AUTHCB_CREATEUSR, "_authCB::createUsr" )
   INT32 _authCB::createUsr( BSONObj &obj, _pmdEDUCB *cb, INT32 w )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_AUTHCB_CREATEUSR ) ;
      rc = _createUsr( obj, cb, w ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_AUTHCB_CREATEUSR, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 _authCB::updatePasswd( const string &user, const string &oldPasswd, 
                                const string &newPasswd, _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      INT64 updatedNum = 0 ;

      BSONObj condition = BSON( SDB_AUTH_USER << user << SDB_AUTH_PASSWD
                                << oldPasswd );
      BSONObj tmp       = BSON( SDB_AUTH_PASSWD << newPasswd ) ;
      BSONObj obj       = BSON( "$set" << tmp ) ;
      {
         SDB_DMSCB *dmsCB = pmdGetKRCB()->getDMSCB() ;
         SDB_DPSCB *dpsCB = pmdGetKRCB()->getDPSCB() ;
         BSONObj hint = BSON( "" << AUTH_USR_INDEX_NAME ) ;
         rc = rtnUpdate( AUTH_USR_COLLECTION, condition, obj, hint,
                         0, cb, dmsCB, dpsCB, 1, &updatedNum ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to update passwd for %s in %s:rc=%d", 
                    user.c_str(), AUTH_USR_COLLECTION, rc ) ;
            goto error ;
         }
         else if ( updatedNum <= 0 )
         {
            PD_LOG( PDERROR, "User name[%s] or password[%s] is error",
                    user.c_str(), oldPasswd.c_str() ) ;
            rc = SDB_AUTH_AUTHORITY_FORBIDDEN ;
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_AUTHCB_REMOVEUSR, "_authCB::removeUsr" )
   INT32 _authCB::removeUsr( BSONObj &obj, _pmdEDUCB *cb, INT32 w )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_AUTHCB_REMOVEUSR ) ;
      SDB_DMSCB *dmsCB = pmdGetKRCB()->getDMSCB() ;
      SDB_DPSCB *dpsCB = pmdGetKRCB()->getDPSCB() ;

      rc = authenticate( obj, cb ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      {
         BSONObj hint = BSON( "" << AUTH_USR_INDEX_NAME ) ;
         rc = rtnDelete( AUTH_USR_COLLECTION,
                         obj, hint,
                         0, cb, dmsCB, dpsCB, w ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "failed to remove usr from AUTH_USR_COLLECTION, "
                    "rc: %d", rc ) ;
            goto error ;
         }
         else
         {
            checkNeedAuth( cb, TRUE ) ;
         }
      }

   done:
      PD_TRACE_EXITRC ( SDB_AUTHCB_REMOVEUSR, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_AUTHCB_INITAUTH, "_authCB::_initAuthentication" )
   INT32 _authCB::_initAuthentication( _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_AUTHCB_INITAUTH ) ;
      SDB_DMSCB *dmsCB = pmdGetKRCB()->getDMSCB() ;

      rc = catTestAndCreateCL( AUTH_USR_COLLECTION, cb, dmsCB, NULL, TRUE ) ;
      if ( rc )
      {
         goto error ;
      }

      {
         BSONObjBuilder builder ;
         builder.append( IXM_FIELD_NAME_KEY, BSON( SDB_AUTH_USER << 1) ) ;
         builder.append( IXM_FIELD_NAME_NAME, AUTH_USR_INDEX_NAME ) ;
         builder.appendBool( IXM_FIELD_NAME_UNIQUE, TRUE ) ;
         BSONObj indexDef = builder.obj() ;

         rc = catTestAndCreateIndex( AUTH_USR_COLLECTION, indexDef, cb, dmsCB,
                                     NULL, TRUE ) ;
         if ( SDB_OK != rc )
         {
            goto error ;
         }
      }

   done:
      PD_TRACE_EXITRC ( SDB_AUTHCB_INITAUTH, rc ) ;
      return rc ;
   error:
      PD_LOG( PDERROR, "failed to init authentication:%d", rc ) ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_AUTHCB_CHECKNEEDAUTH, "_authCB::checkNeedAuth" )
   INT32 _authCB::checkNeedAuth( _pmdEDUCB *cb, BOOLEAN forcecheck )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_AUTHCB_CHECKNEEDAUTH ) ;
      PD_TRACE2 ( SDB_AUTHCB_CHECKNEEDAUTH,
                  PD_PACK_INT ( forcecheck ),
                  PD_PACK_INT ( _needAuth ) ) ;
      SDB_DMSCB *dmsCB = pmdGetKRCB()->getDMSCB() ;
      const CHAR *collection = NULL ;
      dmsStorageUnit *su = NULL ;
      dmsStorageUnitID suID = DMS_INVALID_SUID ;
      SINT64 totalCount = 0 ;
      if ( !forcecheck )
      {
         if ( _needAuth )
         {
            goto done ;
         }
      }
      rc = rtnResolveCollectionNameAndLock( AUTH_USR_COLLECTION,
                                            dmsCB, &su,
                                            &collection, suID ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get AUTH_USR_COLLECTION:%d", rc ) ;
         SDB_ASSERT( FALSE, "impossible" ) ;
         goto error ;
      }
      rc = su->countCollection ( collection, totalCount, cb ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "failed to get count:%d",rc ) ;
         SDB_ASSERT( FALSE, "impossible" ) ;
         goto error ;
      }
      PD_TRACE1 ( SDB_AUTHCB_CHECKNEEDAUTH, PD_PACK_INT ( totalCount ) ) ;
      _needAuth = ( 0 == totalCount ) ? FALSE : TRUE ;

   done:
      if ( DMS_INVALID_SUID != suID )
      {
         dmsCB->suUnlock( suID ) ;
      }
      PD_TRACE_EXITRC ( SDB_AUTHCB_CHECKNEEDAUTH, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_AUTHCB__CREATEUSR, "_authCB::_createUsr" )
   INT32 _authCB::_createUsr( BSONObj &obj, _pmdEDUCB *cb, INT32 w )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_AUTHCB__CREATEUSR ) ;
      SDB_DMSCB *dmsCB = pmdGetKRCB()->getDMSCB() ;
      SDB_DPSCB *dpsCB = pmdGetKRCB()->getDPSCB() ;

      if ( SDB_OK != _valid( obj, TRUE ) )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }

      rc = rtnInsert( AUTH_USR_COLLECTION,
                      obj, 1, 0, cb,
                      dmsCB, dpsCB, w ) ;
      if ( SDB_OK != rc && SDB_IXM_DUP_KEY != rc )
      {
         BSONObj hint ;
         rtnDelete( AUTH_USR_COLLECTION,
                    obj, hint, 0, cb,
                    dmsCB, dpsCB ) ;
         goto error ;
      }
      else if ( SDB_IXM_DUP_KEY == rc )
      {
         goto error ;
      }
      else
      {
         _needAuth = TRUE ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_AUTHCB__CREATEUSR, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_AUTHCB__VALID, "_authCB::_valid" )
   INT32 _authCB::_valid( BSONObj &obj, BOOLEAN notEmpty )
   {
      INT32 rc = SDB_OK ;
      PD_TRACE_ENTRY ( SDB_AUTHCB__VALID ) ;
      BSONElement usr, passwd ;
      INT32 fieldNum = 0 ;
      if ( obj.isEmpty() )
      {
         PD_TRACE0 ( SDB_AUTHCB__VALID ) ;
         goto error ;
      }

      usr = obj.getField( SDB_AUTH_USER ) ;
      if ( usr.eoo() || String != usr.type() ||
           ( usr.String().empty() && notEmpty ) )
      {
         PD_TRACE0 ( SDB_AUTHCB__VALID ) ;
         goto error ;
      }
      ++fieldNum ;

      passwd = obj.getField( SDB_AUTH_PASSWD ) ;
      if ( passwd.eoo() || String != passwd.type() ||
           ( passwd.String().empty() && notEmpty ) )
      {
         PD_TRACE0 ( SDB_AUTHCB__VALID ) ;
         goto error ;
      }
      ++fieldNum ;

      if ( fieldNum != obj.nFields() )
      {
         PD_TRACE0 ( SDB_AUTHCB__VALID ) ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC ( SDB_AUTHCB__VALID, rc ) ;
      return rc ;
   error:
      rc = SDB_INVALIDARG ;
      PD_LOG( PDDEBUG, "invalid obj of the auth[%s]",
              obj.toString().c_str() ) ;
      goto done ;
   }

   /*
      get gloabl SDB_AUTHCB cb
   */
   SDB_AUTHCB* sdbGetAuthCB ()
   {
      static SDB_AUTHCB s_authCB ;
      return &s_authCB ;
   }

}

