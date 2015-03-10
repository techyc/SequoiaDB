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
@description: remove the newly created catalog group
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON:
   SYS_JSON: the format is: { "VCoordSvcName": "10000" }
   ENV_JSON:
@return
   RET_JSON: the format is: {}
*/

var RET_JSON     = new Object() ;
var errMsg       = "" ;
/* *****************************************************************************
@discretion: remove catalog group
@parameter
   db[object]: Sdb object
@return void
***************************************************************************** */
function removeCatalogGroup( db )
{
   var rg = null ;
   try
   {
      rg = db.getCatalogRG() ;
   }
   catch ( e )
   {
      if ( SDB_CLS_GRP_NOT_EXIST == e )
      {
         return ;
      }
      else
      {
         errMsg = "Failed to get catalog group" ;
         exception_handle( e, errMsg ) ;
      }
   }
   // remove
   try
   {
      db.removeCatalogRG() ;
   }
   catch ( e )
   {

      errMsg = "Failed to remove catalog group" ;
      exception_handle( e, errMsg ) ;
   }
}

function main()
{
   var vCoordHostName   = System.getHostName() ;
   var vCoordSvcName    = SYS_JSON[VCoordSvcName] ;
   var db               = null ;
   // connect to virtual coord
   try
   {
      db = new Sdb( vCoordHostName, vCoordSvcName, "", "" ) ;
   }
   catch ( e )
   {
      errMsg = "Failed to connect to temporary coord [" + vCoordHostName + ":" + vCoordSvcName  + "]" ;
      exception_handle( e, errMsg ) ;
   }
   // test whether catalog is running or not
   // if catalog is not running, no need to rollback
   var flag = isCatalogRunning( db ) ;
   if ( !flag )
   {
      return RET_JSON ;
   }
   // remove catalog
   removeCatalogGroup( db ) ;

   return RET_JSON ;
}

// execute
   main() ;

