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

   Source File Name = sptContainer.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          31/03/2014  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef SPT_CONTAINER_HPP_
#define SPT_CONTAINER_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "sptScope.hpp"
#include "ossLatch.hpp"
#include <vector>

namespace engine
{

   typedef std::vector< _sptScope* >            VEC_SCOPE ;
   typedef VEC_SCOPE::iterator                  VEC_SCOPE_IT ;

   #define SPT_OBJ_MASK_STANDARD                0x0001
   #define SPT_OBJ_MASK_USR                     0x0002
   #define SPT_OBJ_MASK_INNER_JS                0x0004

   #define SPT_OBJ_MASK_ALL                     0xFFFF

   class _sptContainer : public SDBObject
   {
   public:
      _sptContainer( INT32 loadMask = SPT_OBJ_MASK_ALL ) ;
      virtual ~_sptContainer() ;

      INT32    init () ;
      INT32    fini () ;

   public:
      _sptScope   *newScope( SPT_SCOPE_TYPE type = SPT_SCOPE_TYPE_SP ) ;
      void        releaseScope( _sptScope *pScope ) ;

   protected:
      _sptScope* _getFromCache( SPT_SCOPE_TYPE type ) ;

      _sptScope* _createScope( SPT_SCOPE_TYPE type ) ;

      INT32      _loadObj( _sptScope *pScope ) ;

   private:
      VEC_SCOPE            _vecScopes ;
      ossSpinXLatch        _latch ;
      INT32                _loadMask ;

   } ;

   typedef class _sptContainer sptContainer ;
}

#endif

