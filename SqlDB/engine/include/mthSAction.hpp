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

   Source File Name = mthSAction.hpp

   Descriptive Name = mth selector action

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          15/01/2015  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef MTH_SACTION_HPP_
#define MTH_SACTION_HPP_

#include "core.hpp"
#include "oss.hpp"
#include "mthDef.hpp"
#include "mthSActionFunc.hpp"
#include "ossUtil.hpp"
#include "mthMatcher.hpp"

namespace engine
{
   class _mthSAction : public SDBObject
   {
   public:
      _mthSAction() ;
      ~_mthSAction() ;

   public:
      OSS_INLINE void setValue( const bson::BSONElement &e )
      {
         _value = e ;
         return ;
      }

      OSS_INLINE const bson::BSONElement &getValue() const
      {
         return _value ;
      }

      OSS_INLINE void setFunc( MTH_SACTION_BUILD buildFunc,
                               MTH_SACTION_GET getFunc )
      {
         _buildFunc = buildFunc ;
         _getFunc = getFunc ;
         return ;
      }
      
      OSS_INLINE void setName( const CHAR *name )
      {
         _name = name ;
         return ;
      }

      OSS_INLINE const CHAR *getName() const
      {
         return _name ;
      }

      OSS_INLINE void setAttribute( MTH_S_ATTRIBUTE attr )
      {
         _attribute = attr ;
         return ;
      }

      OSS_INLINE UINT32 getAttribute() const
      {
         return _attribute ;
      }

      OSS_INLINE const bson::BSONObj &getObj()const
      {
         return _obj ;
      }

      OSS_INLINE void setObj( const bson::BSONObj &obj )
      {
         _obj = obj ;
         return ;
      }

      OSS_INLINE void setSlicePair( INT32 begin,
                                    INT32 step )
      {
         _begin = begin ;
         _limit = step ;
         return ;
      }

      OSS_INLINE void getSlicePair( INT32 &begin,
                                    INT32 &step )
      {
         begin = _begin ;
         step = _limit ;
         return ;
      }

      OSS_INLINE void clear()
      {
         _value = bson::BSONElement() ;
         _name = NULL ;
         _attribute = MTH_S_ATTR_NONE ;
         return ;
      }

      OSS_INLINE BOOLEAN empty() const
      {
         return !MTH_ATTR_IS_VALID( _attribute ) ;
      }

      OSS_INLINE _mthMatcher &getMatcher()
      {
         return _matcher ;
      }
   public:
      INT32 build( const CHAR *name,
                   const bson::BSONElement &e,
                   bson::BSONObjBuilder &builder ) ;

      INT32 get( const CHAR *name,
                 const bson::BSONElement &in,
                 bson::BSONElement &out ) ;

   private:
      MTH_SACTION_BUILD _buildFunc ;
      MTH_SACTION_GET _getFunc ;      

   private:
      bson::BSONElement _value ;
      const CHAR *_name ;
      MTH_S_ATTRIBUTE _attribute ;

      bson::BSONObj _obj ;
      _mthMatcher _matcher ;
      INT32 _begin ;
      INT32 _limit ;
      
   } ;
   typedef class _mthSAction mthSAction ;
}

#endif

