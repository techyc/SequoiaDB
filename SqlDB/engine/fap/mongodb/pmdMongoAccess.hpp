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

   Source File Name = aggrGroup.hpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains functions for agent processing.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          01/27/2015  LZ  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef _SDB_MONGO_ACCESS_HPP_
#define _SDB_MONGO_ACCESS_HPP_

#include "pmdAccessProtocolBase.hpp"
#include "sdbInterface.hpp"
#include "ossFeat.hpp"

#define ACCESS_FOR_MONGODB_CLIENT "server for mongodb client"
#define PORT_OFFSET 7

class _pmdMongoAccess : public engine::IPmdAccessProtocol
{
public:
   _pmdMongoAccess() {}
   virtual ~_pmdMongoAccess() {}

   virtual const CHAR *name() const
   {
      return ACCESS_FOR_MONGODB_CLIENT;
   }


public:
   virtual INT32 init( engine::IResource *pResource ) ;
   virtual INT32 active() ;
   virtual INT32 deactive() ;
   virtual INT32 fini() ;

   virtual const CHAR *getServiceName() const ;
   virtual engine::pmdSession *getSession( SOCKET fd,
                                           engine::IProcessor *pProcessor ) ;
   virtual void releaseSession( engine::pmdSession *pSession ) ;

private:
   void _release() ;

private:
   engine::IResource *_resource ;
   CHAR _serviceName[ OSS_MAX_SERVICENAME + 1 ] ;
};

typedef _pmdMongoAccess pmdMongoAccess ;
#endif
