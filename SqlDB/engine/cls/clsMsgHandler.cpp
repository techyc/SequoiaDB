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

   Source File Name = clsMsgHandler.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          26/11/2012  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#include "clsMsgHandler.hpp"
#include "pdTrace.hpp"
#include "clsTrace.hpp"


namespace engine
{
   /*
      _shdMsgHandler implement
   */
   _shdMsgHandler::_shdMsgHandler ( _pmdAsycSessionMgr *pSessionMgr )
      : _pmdAsyncMsgHandler ( pSessionMgr )
   {
      _pShardCB = NULL ;
   }

   _shdMsgHandler::~_shdMsgHandler ()
   {
      _pShardCB = NULL ;
   }

   void _shdMsgHandler::_postMainMsg( const NET_HANDLE & handle,
                                      MsgHeader * pNewMsg )
   {
      if ( _pShardCB && ( MSG_CAT_NODEGRP_RES == (UINT32)pNewMsg->opCode ||
           MSG_CAT_QUERY_CATALOG_RSP == (UINT32)pNewMsg->opCode ||
           MSG_CAT_QUERY_SPACEINFO_RSP == (UINT32)pNewMsg->opCode ||
           ( MSG_CAT_CATGRP_RES == (UINT32)pNewMsg->opCode &&
             _pShardCB->getTID() != (UINT32)pNewMsg->requestID ) ) )
      {
         _pShardCB->postEvent( pmdEDUEvent( PMD_EDU_EVENT_MSG,
                                            PMD_EDU_MEM_ALLOC,
                                            pNewMsg, (UINT64)handle ) ) ;
      }
      else
      {
         pNewMsg->TID = (UINT32)CLS_SHARD ;
         _pMgrEDUCB->postEvent( pmdEDUEvent( PMD_EDU_EVENT_MSG,
                                             PMD_EDU_MEM_ALLOC,
                                             pNewMsg, (UINT64)handle ) );
      }
   }

   /*
      _replMsgHandler implement
   */
   _replMsgHandler::_replMsgHandler ( _pmdAsycSessionMgr *pSessionMgr )
      :_pmdAsyncMsgHandler ( pSessionMgr )
   {
   }

   _replMsgHandler::~_replMsgHandler ()
   {
   }

   void _replMsgHandler::_postMainMsg( const NET_HANDLE &handle,
                                       MsgHeader *pNewMsg )
   {
      pNewMsg->TID = (UINT32)CLS_REPL ;
      _pMgrEDUCB->postEvent( pmdEDUEvent( PMD_EDU_EVENT_MSG,
                                          PMD_EDU_MEM_ALLOC,
                                          pNewMsg, (UINT64)handle ) ) ;
   }

}


