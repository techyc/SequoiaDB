/*******************************************************************************
   Copyright (C) 2012-2014 SqlDB Ltd.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*******************************************************************************/

#ifndef NETWORK_H__
#define NETWORK_H__

#include "core.h"
SDB_EXTERN_C_START
INT32 clientConnect ( const CHAR *pHostName,
                      const CHAR *pServiceName,
                      SOCKET *sock ) ;

void clientDisconnect ( SOCKET sock ) ;

INT32 clientSend ( SOCKET sock, const CHAR *pMsg, INT32 len, INT32 timeout ) ;
INT32 clientRecv ( SOCKET sock, CHAR *pMsg, INT32 len, INT32 timeout ) ;

INT32 disableNagle( SOCKET sock ) ;
SDB_EXTERN_C_END
#endif
