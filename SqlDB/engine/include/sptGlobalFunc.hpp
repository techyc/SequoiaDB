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

   Source File Name = sptGlobalFunc.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          31/03/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef SPT_BLOBALFUNC_HPP_
#define SPT_BLOBALFUNC_HPP_

#include "sptApi.hpp"

namespace engine
{
   class _sptGlobalFunc : public SDBObject
   {
   JS_DECLARE_CLASS( _sptGlobalFunc )

   public:
      _sptGlobalFunc() {}
      virtual ~_sptGlobalFunc() {} 

   public:
      static INT32 getLastErrorMsg( const _sptArguments &arg,
                                    _sptReturnVal &rval,
                                    bson::BSONObj &detail ) ;

      static INT32 setLastErrorMsg( const _sptArguments &arg,
                                    _sptReturnVal &rval,
                                    bson::BSONObj &detail ) ;

      static INT32 getLastError( const _sptArguments &arg,
                                 _sptReturnVal &rval,
                                 bson::BSONObj &detail ) ;

      static INT32 setLastError( const _sptArguments &arg,
                                 _sptReturnVal &rval,
                                 bson::BSONObj &detail ) ;

      static INT32 sleep( const _sptArguments &arg,
                          _sptReturnVal &rval,
                          bson::BSONObj &detail ) ;
   } ;
   typedef class _sptGlobalFunc sptGlobalFunc ;
}

#endif

