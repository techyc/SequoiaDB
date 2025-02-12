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

   Source File Name = ossFeat.h

   Descriptive Name =

   When/how to use:

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/28/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef OSSFEAT_H_
#define OSSFEAT_H_

/**
 *   \file ossfeat.h
 *   \brief Operating system specific features
 *
 */

#if defined (_WIN32)
   #define _WINDOWS
   #include <ws2tcpip.h>
   #include <Windows.h>
   #include <WinBase.h>
   #define __FUNC__ __FUNCTION__
#endif

#if defined (_WIN32) && !defined (_WIN64)
   #define _WINDOWS32
#elif defined (_WIN64)
   #define _WINDOWS64
#endif

#if defined(__linux__)  && defined(__i386__)
   #define _LIN32
   #define _LINUX
#elif defined(__linux__) && (defined(__ia64__)||defined(__x86_64__))
   #define _LIN64
   #define _LINUX
#elif defined(__linux__) && (defined(__PPC64__))
   #define _PPCLIN64
   #define _LINUX
#endif

#if defined ( _WINDOWS32 ) || defined ( _LIN32 )
   #define OSS_ARCH_32
#elif defined ( _WINDOWS64 ) || defined ( _LIN64 ) || defined ( _PPCLIN64 )
   #define OSS_ARCH_64
#endif

#define OSS_OSTYPE_WIN32               0
#define OSS_OSTYPE_WIN64               1
#define OSS_OSTYPE_LIN32               2
#define OSS_OSTYPE_LIN64               3
#define OSS_OSTYPE_PPCLIN64            4
#if defined (_WINDOWS32)
#define OSS_OSTYPE                     OSS_OSTYPE_WIN32
#elif defined (_WINDOWS64)
#define OSS_OSTYPE                     OSS_OSTYPE_WIN64
#elif defined (_LIN32)
#define OSS_OSTYPE                     OSS_OSTYPE_LIN32
#elif defined (_LIN64)
#define OSS_OSTYPE                     OSS_OSTYPE_LIN64
#elif defined (_PPCLIN64)
#define OSS_OSTYPE                     OSS_OSTYPE_PPCLIN64
#endif


#if defined _LINUX
   #include <errno.h>
   #define OSS_HAS_KERNEL_THREAD_ID
   #define __FUNC__ __func__
   #define SDB_EXPORT

   #define OSS_FD_SETSIZE  65528
   #include <bits/types.h>
   #include <linux/posix_types.h>

   #if defined (SDB_ENGINE) || defined (SDB_CLIENT)
      #ifdef _SYS_SELECT_H
      # error "Can't include <sys/select.h> before the file"
      #endif //_SYS_SELECT_H
   #endif //SDB_ENGINE || SDB_CLIENT

   #undef __FD_SETSIZE
   #define __FD_SETSIZE    OSS_FD_SETSIZE
   #undef FD_SETSIZE
   #define FD_SETSIZE      __FD_SETSIZE
   #include <sys/types.h>

#elif defined _WINDOWS
   #ifdef SDB_STATIC_BUILD
      #define SDB_EXPORT
   #elif defined SDB_DLL_BUILD
      #define SDB_EXPORT __declspec(dllexport)
   #else
      #define SDB_EXPORT __declspec(dllimport)
   #endif
   #define OSS_FD_SETSIZE  FD_SETSIZE
   #include <sys/types.h>
#endif

#endif /* OSSFEAT_H_ */

