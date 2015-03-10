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
@description: create catalog
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is: { "InstallHostName": "rhel64-test9", "InstallSvcName": "11900", "InstallPath": "/opt/sqldb/database/catalog/11900", "InstallConfig": { "diaglevel": 3, "role": "catalog", "logfilesz": 64, "logfilenum": 20, "transactionon": "false", "preferedinstance": "A", "numpagecleaners": 1, "pagecleaninterval": 10000, "hjbuf": 128, "logbuffsize": 1024, "maxprefpool": 200, "maxreplsync": 10, "numpreload": 0, "sortbuf": 512, "syncstrategy": "none" } } ;
   SYS_JSON: the format is: { "VCoordSvcName": "10000", "SdbUser": "sdbadmin", "SdbPasswd": "sdbadmin", "SdbUserGroup": "sdbadmin_group", "User": "root", "Passwd": "sqldb", "SshPort": "22" } ;
@return
   RET_JSON: the format is: {}
*/

var RET_JSON        = new Object() ;
var errMsg          = "" ;
/* *****************************************************************************
@discretion: wait catalog to be ok
@author: Tanzhaobo
@parameter
   hostName[string]: the newly build catalog host name
   svcName[string]: the newly build catalog svc name
@return void
***************************************************************************** */
function waitCatalogRGReady( hostName, svcName )
{
   var db  = null ;
   var cur = null ;
   var num = 0 ;
   var i   = 0 ;
   for ( ; i < OMA_WAIT_CATA_RG_TRY_TIMES; i++ )
   {
      try
      {
         db = new Sdb( hostName, svcName ) ;
         cur = db.SYSCAT.SYSNODES.find() ;
         num = cur.size() ;
         if ( num )
         {
            // for cur.size() had run out the cursor, we need to get again
            cur = db.SYSCAT.SYSNODES.find({"GroupName": "SYSCatalogGroup"}) ;
            var record = eval ( '(' + cur.next() + ')' ) ;
            var n = record[PrimaryNode] ;
            if ( "undefined" == typeof(n) )
            {
               sleep( OMA_SLEEP_TIME ) ;
               continue ;   
            }
            else
            {
               break ;
            }
         }
         else
         {
            sleep( OMA_SLEEP_TIME ) ;
            continue ; 
         }
      }
      catch ( e )
      {
         sleep( OMA_SLEEP_TIME ) ;
         continue ;
      }
   }
   if ( OMA_WAIT_CATA_RG_TRY_TIMES <= i )
   {
      setLastErrMsg( "Wait catalog to be ready timeout" ) ;
      setLastError( SDB_SYS ) ;
      throw SDB_SYS ;
   }
}

/* *****************************************************************************
@discretion: create catalog
@parameter
   db[object]: Sdb object
   hostName[string]: install host name
   svcName[string]: install svc name
   installPath[string]: install path
   config[json]: config info 
@return void
***************************************************************************** */
function createCatalogNode( db, hostName, svcName, installPath, config )
{
   // try to get system catalog group
   var rg = null ;
   var node = null ;
   try
   {
      rg = db.getRG( OMA_SYS_CATALOG_RG ) ;
   }
   // catalog has not been created
   catch ( e )
   {
      if ( SDB_CAT_NO_ADDR_LIST == e )
      {
         try
         {
            rg = db.createCataRG( hostName, svcName,
                                  installPath, config ) ;
            return ;
         }
         catch ( e )
         {
            errMsg = "Failed to create catalog group" ;
            exception_handle( e, errMsg ) ;
         }
      }
      else
      {
         errMsg = "Failed to get catalog group" ;
         exception_handle( e, errMsg ) ;
      }
   }
   // catalog has been created
   try
   {
      node = rg.createNode( hostName, svcName, installPath, config ) ;
   }
   catch ( e )
   {
      errMsg = "Failed to create catalog node [" + hostName + ":" + svcName + "]" ;
      exception_handle( e, errMsg ) ;
   }
   // start catalog node
   try
   {
      node.start() ;
   }
   catch ( e )
   {
      errMsg = "Failed to start catalog node [" + hostName + ":" + svcName + "]" ;
      exception_handle( e, errMsg ) ;
   }
}

function main()
{
   var vCoordHostName  = System.getHostName() ;
   var vCoordSvcName   = SYS_JSON[VCoordSvcName] ;
   var sdbUser         = SYS_JSON[SdbUser] ;
   var sdbUserGroup    = SYS_JSON[SdbUserGroup] ;
   var user            = SYS_JSON[User] ;
   var passwd          = SYS_JSON[Passwd] ;    
   var sshport         = parseInt(SYS_JSON[SshPort]) ;
   var installHostName = BUS_JSON[InstallHostName] ;
   var installSvcName  = BUS_JSON[InstallSvcName] ;
   var installPath     = BUS_JSON[InstallPath] ;
   var installConfig   = BUS_JSON[InstallConfig] ;
   var db              = null ;
   var ssh             = new Ssh( installHostName, user, passwd, sshport ) ;
   var osInfo          = System.type() ; 
   // change install path owner
   changeDirOwner( ssh, osInfo, installPath, sdbUser, sdbUserGroup ) ;
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
   // create catalog node
   createCatalogNode( db, installHostName, installSvcName,
                      installPath, installConfig ) ;
   // wait catalog to be available
   waitCatalogRGReady( installHostName, installSvcName ) ; 
   return RET_JSON ;
}

// execute
   main() ;

