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

   Source File Name = rtnSQLSum.cpp

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

#include "rtnSQLSum.hpp"

namespace engine
{
   _rtnSQLSum::_rtnSQLSum()
   :_sum(0),
    _effective( FALSE )
   {
      _name = "sum" ;
   }

   _rtnSQLSum::~_rtnSQLSum()
   {

   }

   INT32 _rtnSQLSum::result( BSONObjBuilder &builder )
   {
      INT32 rc = SDB_OK ;
      try
      {
         if ( !_effective )
         {
            builder.appendNull( _alias.toString() ) ;
         }
         else
         {
            builder.append( _alias.toString(), _sum ) ;
            _effective = FALSE ;
            _sum = 0 ;
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDERROR, "unexcepted err happened%s",
                 e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _rtnSQLSum::_push( const RTN_FUNC_PARAMS &param )
   {
      INT32 rc = SDB_OK ;
      const BSONElement &ele = *(param.begin()) ;
      if ( !ele.eoo() && ele.isNumber() )
      {
         _sum += ele.Number() ;
         _effective = TRUE ;
      }
      return rc ;
   }
}
