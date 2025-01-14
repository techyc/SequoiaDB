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

   Source File Name = qgmSelector.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains functions for agent processing.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  YW  Initial Draft

   Last Changed =

******************************************************************************/

#include "qgmSelector.hpp"
#include "pd.hpp"
#include "pdTrace.hpp"
#include "qgmTrace.hpp"
#include <sstream>

namespace engine
{
   _qgmSelector::_qgmSelector()
   :_hasAlias( FALSE )
   {

   }

   _qgmSelector::~_qgmSelector()
   {
      _selector.clear() ;
   }

   string _qgmSelector::toString() const
   {
      stringstream ss ;
      if ( !_selector.empty() )
      {
         ss << "[" ;
         qgmOPFieldVec::const_iterator itr = _selector.begin() ;
         for ( ; itr != _selector.end(); itr++ )
         {
            if ( SQL_GRAMMAR::WILDCARD == itr->type )
            {
               ss << "{value:*}," ;
            }
            else
            {
               ss << "{value:" << itr->value.toString()
                  << ",alias:" << itr->alias.toString()
                 << "}," ;
            }
         }
         ss.seekp((INT32)ss.tellp()-1 ) ;
         ss << "]" ;
      }
      else
      {
         ss << "[*]" ;
      }
      return ss.str() ;
   }

   INT32 _qgmSelector::load( const qgmOPFieldVec &op )
   {
      INT32 rc = SDB_OK ;
      qgmOPFieldVec::const_iterator itr = op.begin() ;
      for ( ; itr != op.end(); itr++ )
      {
         _selector.push_back( *itr ) ;
         if ( !( itr->alias.empty() ) )
         {
            _hasAlias = TRUE ;
         }
      }
      return rc ;
   }

   // PD_TRACE_DECLARE_FUNCTION( SDB__QGMSELECTOR_SELECT, "_qgmSelector::select" )
   INT32 _qgmSelector::select( const BSONObj &src, BSONObj &out ) const
   {
      PD_TRACE_ENTRY( SDB__QGMSELECTOR_SELECT ) ;
      INT32 rc = SDB_OK ;
      if ( _selector.empty() )
      {
         out = src ;
         goto done ;
      }
      try
      {
         BSONObjBuilder builder ;
         qgmOPFieldVec::const_iterator itr = _selector.begin() ;
         for ( ; itr != _selector.end(); itr++ )
         {
            if ( SQL_GRAMMAR::WILDCARD == itr->type )
            {
               out = src ;
               goto done ;
            }
            {
            string fieldName = itr->value.attr().toFieldName() ;
            BSONElement ele = src.getFieldDotted( fieldName ) ;
            if ( ele.eoo() )
            {
               if ( itr->alias.empty() )
               {
                  builder.appendNull( fieldName ) ;
               }
               else
               {
                  builder.appendNull( itr->alias.toString() ) ;
               }
            }
            else
            {
               if ( itr->alias.empty() )
               {
                  builder.append( ele ) ;
               }
               else
               {
                  builder.appendAs( ele, itr->alias.toString() ) ;
               }
            }
            }
         }

         out = builder.obj() ;
      }
      catch (std::exception &e)
      {
         PD_LOG( PDERROR, "unexcepted err happened:%s",
                 e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMSELECTOR_SELECT, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION( SDB__QGMSELECTOR_SELECT2, "_qgmSelector::select" )
   INT32 _qgmSelector::select( const qgmFetchOut &src,
                               BSONObj &out ) const
   {
      PD_TRACE_ENTRY( SDB__QGMSELECTOR_SELECT2 ) ;
      INT32 rc = SDB_OK ;
      if ( _selector.empty() )
      {
         out = src.mergedObj() ;
         goto done ;
      }
      else
      {
         BSONObjBuilder builder ;
         BSONElement ele ;
         try
         {
            qgmOPFieldVec::const_iterator itr = _selector.begin() ;
            for ( ; itr != _selector.end(); itr++ )
            {
               if ( SQL_GRAMMAR::WILDCARD == itr->type )
               {
                  out = src.mergedObj() ;
                  goto done ;
               }

               rc = src.element( itr->value, ele ) ;
               if ( rc )
               {
                  goto error ;
               }
               else if ( ele.eoo() )
               {
                  if ( itr->alias.empty() )
                  {
                     builder.appendNull( itr->value.attr().toFieldName() ) ;
                  }
                  else
                  {
                     builder.appendNull( itr->alias.toString() ) ;
                  }
               }
               else if ( itr->alias.empty() )
               {
                  builder.append( ele ) ;
               }
               else
               {
                  builder.appendAs( ele, itr->alias.toString() ) ;
               }
            }

            out = builder.obj() ;
         }
         catch ( std::exception & e )
         {
            PD_LOG( PDERROR, "unexcepted err happened:%s",
                    e.what() ) ;
            rc = SDB_SYS ;
            goto error ;
         }
      }
   done:
      PD_TRACE_EXITRC( SDB__QGMSELECTOR_SELECT2, rc ) ;
      return rc ;
   error:
      goto done ;
   }

   BSONObj _qgmSelector::selector() const
   {
      BSONObjBuilder builder ;

      qgmOPFieldVec::const_iterator itr = _selector.begin() ;
      for ( ; itr != _selector.end(); itr++ )
      {
         if ( !itr->value.attr().empty() )
         {
            builder.appendNull( itr->value.attr().toString()) ;
         }
      }

      return builder.obj() ;
   }
}
