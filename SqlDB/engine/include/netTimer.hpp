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

   Source File Name = netTimer.hpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-motionatted
   versions of PD component. This file contains declare of PD functions.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef NETTIMER_HPP_
#define NETTIMER_HPP_
#include "core.hpp"
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include "oss.hpp"
#include "netDef.hpp"

namespace engine
{
   class _netTimeoutHandler : public SDBObject
   {
      public:
         virtual ~_netTimeoutHandler(){}
      public:
         virtual void handleTimeout( const UINT32 &millisec,
                                     const UINT32 &id ) = 0;
   };

   class _netTimer :
         public boost::enable_shared_from_this<_netTimer>,
         public SDBObject
   {
      public:
         _netTimer( UINT32 millisec, UINT32 id, boost::asio::io_service &io,
                    _netTimeoutHandler *handler ):
                  _timer( io, boost::posix_time::milliseconds(millisec)),
                  _handler(handler),
                  _id(id),
                  _millisec(millisec),
                  _actived(TRUE)
         {

         }

         ~_netTimer()
         {
         }
      public:
         OSS_INLINE void timeoutCallback( const boost::system::error_code &
                                      error )
         {
            if ( !error )
            {
               _handler->handleTimeout( _millisec, _id ) ;
            }
            asyncWait() ;
            return ;
         }
         OSS_INLINE UINT32 id()
         {
            return _id ;
         }
         OSS_INLINE UINT32 timeout()
         {
            return _millisec ;
         }
         OSS_INLINE void asyncWait()
         {
            if ( !_actived )
            {
               return ;
            }
            _timer.expires_from_now(boost::posix_time::milliseconds(_millisec));
            _timer.async_wait(boost::bind(&_netTimer::timeoutCallback,
                                          shared_from_this(),
                                          boost::asio::placeholders::error));
         }
         OSS_INLINE void cancel()
         {
            _actived = FALSE ;
            _timer.cancel() ;
         }

      private:
         boost::asio::deadline_timer _timer;
         _netTimeoutHandler *_handler ;
         UINT32 _id ;
         UINT32 _millisec ;
         BOOLEAN _actived ;
   };

   typedef boost::shared_ptr<_netTimer> NET_TH ;
}

#endif

