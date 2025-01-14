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

   Source File Name = rtnLobWindow.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/31/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef RTN_LOBWINDOW_HPP_
#define RTN_LOBWINDOW_HPP_

#include "dmsLobDef.hpp"
#include "rtnLobTuple.hpp"

namespace engine
{
   class _rtnLobWindow : public SDBObject
   {
   public:
      _rtnLobWindow() ;
      virtual ~_rtnLobWindow() ;

   public:
      INT32 init( INT32 pageSize ) ;

      INT32 prepare2Write( SINT64 offset, UINT32 len, const CHAR *data ) ;

      BOOLEAN getNextWriteSequences( RTN_LOB_TUPLES &tuples ) ;

      void cacheLastDataOrClearCache() ;

      BOOLEAN getCachedData( _rtnLobTuple &tuple ) ;

      
      INT32 prepare2Read( SINT64 lobLen,
                          SINT64 offset,
                          UINT32 len,
                          RTN_LOB_TUPLES &tuples ) ;
   private:
      BOOLEAN _getNextWriteSequence( _rtnLobTuple &tuple ) ;

   private:
      SINT32 _pageSize ;
      UINT32 _logarithmic ;

      SINT64 _curOffset ;
      CHAR *_pool ;
      INT32 _cachedSz ;

      _rtnLobTuple _writeData ;
      BOOLEAN _analysisCache ;
   } ;
   typedef class _rtnLobWindow rtnLobWindow ;
}

#endif

