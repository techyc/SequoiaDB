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

   Source File Name = rtnSQLMax.hpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains declare for QGM operators

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          04/09/2013  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef RTNSQLMAX_HPP_
#define RTNSQLMAX_HPP_

#include "rtnSQLFunc.hpp"

namespace engine
{
   class _rtnSQLMax : public _rtnSQLFunc
   {
   public:
      _rtnSQLMax() ;
      virtual ~_rtnSQLMax() ;
   public:
      INT32 result( BSONObjBuilder &builder ) ;

   private:
      INT32 _push( const RTN_FUNC_PARAMS &param ) ;
   private:
      BSONObj _obj ;
      BSONElement _ele ;
   } ;
}

#endif

