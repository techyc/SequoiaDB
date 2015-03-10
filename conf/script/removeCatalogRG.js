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
/*
@description: remove coord group
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is: { "AuthUser": "", "AuthPasswd": "" }
   SYS_JSON: the format is: { "VCoordSvcName": "10000" }
@return
   RET_JSON: the format is: {}
*/

var RET_JSON = new Object() ;
var errMsg   = "" ;
function main()
{
   var localHostName = System.getHostName() ;
   var vCoordSvcName = SYS_JSON[VCoordSvcName] ;
   var authUser      = BUS_JSON[AuthUser] ;
   var authPasswd    = BUS_JSON[AuthPasswd] ;

   var db = new Sdb ( localHostName, vCoordSvcName, authUser, authPasswd ) ;
   try
   {
      // remove catalog group
      db.removeCatalogRG() ;
   }
   catch ( e )
   {
      errMsg = "Failed to remove catalog group" ;
      exception_handle( e, errMsg ) ;
   }

   return RET_JSON ;
}

// execute
   main() ;

