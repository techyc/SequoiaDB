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

   Source File Name = rtnLobFetcher.hpp

   Descriptive Name =

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          07/31/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef RTN_LOBFETCHER_HPP_
#define RTN_LOBFETCHER_HPP_

#include "dmsLobDef.hpp"
#include "rtn.hpp"

namespace engine
{
   class _rtnLobFetcher : public SDBObject
   {
   public:
      _rtnLobFetcher() ;
      ~_rtnLobFetcher() ;

   public:
      INT32 init( const CHAR *fullName,
                  BOOLEAN onlyMetaPage ) ;

      INT32 fetch( _pmdEDUCB *cb,
                   _dmsLobInfoOnPage &piece,
                   _dpsMessageBlock *mb = NULL ) ;

      BOOLEAN hitEnd() const
      {
         return _hitEnd ;
      }

      DMS_LOB_PAGEID toBeFetched() const
      {
         return _pos ;
      }

      _dmsStorageUnit *getSu()
      {
         return _su ;
      }

   private:
      void _fini() ;
   private:
      dmsStorageUnitID _suID ;
      _dmsStorageUnit *_su ;
      _dmsMBContext *_mbContext ;
      DMS_LOB_PAGEID _pos ;
      BOOLEAN _onlyMetaPage ;
      BOOLEAN _hitEnd ;
   } ;
   typedef class _rtnLobFetcher rtnLobFetcher ;
}

#endif

