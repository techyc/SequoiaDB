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

   Source File Name = dmsLobDirectOutBuffer.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/31/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef DMS_LOBDIRECTOUTBUFFER_HPP_
#define DMS_LOBDIRECTOUTBUFFER_HPP_

#include "dmsLobDirectBuffer.hpp"

namespace engine
{
   class _dmsLobDirectOutBuffer : public _dmsLobDirectBuffer 
   {
   public:
      _dmsLobDirectOutBuffer( const void *buf,
                              UINT32 size,
                              _pmdEDUCB *cb ) ;
      virtual ~_dmsLobDirectOutBuffer() ;
   public:
      virtual INT32 getAlignedTuple( tuple &t ) ;

   private:
      const void *_usrBuf ;
      UINT32 _size ;
   } ;
   typedef class _dmsLobDirectOutBuffer dmsLobDirectOutBuffer ;
}

#endif

