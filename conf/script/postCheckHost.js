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
@description: After check the target host, clean up the environment in target host.
              Here, we are going to remove the processes and js files, and drop the
              temporary director
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is: { "HostInfo": [ { "IP": "192.168.20.165", "HostName": "rhel64-test8", "User": "root", "Passwd": "sqldb", "InstallPath": "/opt/sqldb", "SshPort": "22", "AgentService": "11790" }, { "IP": "192.168.20.166", "HostName": "rhel64-test9", "User": "root", "Passwd": "sqldb", "InstallPath": "/opt/sqldb", "SshPort": "22", "AgentService": "11790" } ] }
   SYS_JSON:
   ENV_JSON:
   OTHER_JSON:
@return
   RET_JSON: the format is:  { "HostInfo": [ { "IP": "192.168.20.165", "errno": 0, "detail": "" }, { "IP": "192.168.20.166", "errno": 0, "detail": "" } ] }
*/

//println
//var BUS_JSON = { "HostInfo": [ { "IP": "192.168.20.42", "HostName": "susetzb", "User": "root", "Passwd": "sqldb", "InstallPath": "/opt/sqldb", "SshPort": "22", "AgentService": "11790" }, { "IP": "192.168.20.165", "HostName": "rhel64-test8", "User": "root", "Passwd": "sqldb", "InstallPath": "/opt/sqldb", "SshPort": "22", "AgentService": "11790" } ] } ;

var FILE_NAME_POST_CHECK_HOST = "postCheckHost.js" ;
var RET_JSON       = new Object() ;
RET_JSON[HostInfo] = [] ;
var rc             = SDB_OK ;
var errMsg         = "" ;

/* *****************************************************************************
@discretion: stop temporary sdbcm in remote host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
@return void
***************************************************************************** */
function _stopTmpSdbcm( ssh )
{
   var str = ""
   try
   {
      if ( SYS_LINUX == SYS_TYPE )
      {
         str += OMA_PATH_TEMP_BIN_DIR_L ;
         str += OMA_PROG_SDBCMTOP_L ;
         str += " " + OMA_OPTION_SDBCMART_I ;
         
         ssh.exec( str ) ;
      }
      else
      {
         // TODO:
      }
   }
   catch( e )
   {
   }
}

function main()
{
   PD_LOG( arguments, PDEVENT, FILE_NAME_POST_CHECK_HOST, "Begin to post-check host" ) ;
   
   var infoArr = null ;
   var arrLen = null ;
   
   try
   {
      infoArr = BUS_JSON[HostInfo] ;
      arrLen = infoArr.length ;
   }
   catch( e )
   {
      SYSEXPHANDLE( e ) ;
      rc = GETLASTERROR() ;
      errMsg = "Js receive invalid argument" ;
      // record error message in log
      PD_LOG( arguments, PDERROR, FILE_NAME_POST_CHECK_HOST,
              sprintf( errMsg + ", rc: ?, error: ?", rc, GETLASTERRMSG() )  ) ;
      // tell to user error happen
      exception_handle( SDB_INVALIDARG, errMsg ) ;
   }
   if ( arrLen == 0 )
   {
      errMsg = "Not specified any host to post-check" ;
      PD_LOG( arguments, PDERROR, FILE_NAME_POST_CHECK_HOST, errMsg ) ;
      exception_handle( SDB_INVALIDARG, errMsg ) ;
   }

   for ( var i = 0; i < arrLen; i++ )
   {
      var ssh        = null ;
      var obj        = null ;
      var ip         = null ;
      var user       = null ;
      var passwd     = null ;
      var sshport    = null ;
      var retObj     = new postCheckResult() ;
      
      try
      {
         obj         = infoArr[i]
         ip          = obj[IP] ;
         user        = obj[User] ;
         passwd      = obj[Passwd] ;
         sshport     = parseInt(obj[SshPort]) ;
         retObj[IP]  = ip ;
         
         // 1. ssh
         ssh = new Ssh( ip, user, passwd, sshport ) ;
/*
         // check whether it is in local host,
         // if so, we would not stop local sdbcm
         var flag = isInLocalHost( ssh ) ;
         if ( flag )
         {
            RET_JSON[HostInfo].push( retObj ) ;
            continue ;
         }
*/
         // 2. try to remove temporary sdbcm in target host
// println
//         _stopTmpSdbcm( ssh ) ;
         
         // 3. remove the temporary directory in target host but leave the log file
         removeTmpDir2( ssh ) ;
      }
      catch ( e )
      {
         SYSEXPHANDLE( e ) ;
         retObj[IP] = ip ;
         retObj[Errno] = GETLASTERROR() ;
         retObj[Detail] = GETLASTERRMSG() ;
         PD_LOG( arguments, PDERROR, FILE_NAME_POST_CHECK_HOST,
                 "Failed to post-check host[" + ip + "], rc: " + retObj[Errno] + ", detail: " + retObj[Detail] ) ;

         // remove tmp dir and stop sdbcm anyway
         try
         {
// println
//            _stopTmpSdbcm( ssh ) ;
         }
         catch( e ){}
         try
         {
            removeTmpDir2( ssh ) ;
         }
         catch( e ){}
      }
      
      RET_JSON[HostInfo].push( retObj ) ;
   }

   PD_LOG( arguments, PDEVENT, FILE_NAME_POST_CHECK_HOST, "Finish post-checking host" ) ;
println("RET_JSON is: " + JSON.stringify(RET_JSON)) ;
   // return the result
   return RET_JSON ;
}

// execute
   main() ;

