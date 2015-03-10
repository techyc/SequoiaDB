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
@description: Prepare for check target host. We are going to push some tool
              processes and js files to target host, and then start a temporary
              sdbcm there
@modify list:
   2014-7-26 Zhaobo Tan  Init
@parameter
   BUS_JSON: the format is: { "HostInfo": [ { "IP": "192.168.20.42", "HostName": "susetzb", "User": "root", "Passwd": "sqldb", "SshPort": "22" }, { "IP": "192.168.20.165", "HostName": "rhel64-test8", "User": "root", "Passwd": "sqldb", "SshPort": "22" } ] } ;
   SYS_JSON: the path where we get the tools processes from,
             the format is: { "ProgPath": "/opt/sqldb/bin/" } ;
   ENV_JSON:
@return
   RET_JSON: the format is: { "HostInfo": [ { "errno": 0, "detail": "", "AgentPort": "10000", "IP": "192.168.20.42" }, { "errno": 0, "detail": "", "AgentPort": "10000", "IP": "192.168.20.165" } ] }
*/

//println
//var BUS_JSON = { "HostInfo": [ { "IP": "192.168.20.42", "HostName": "susetzb", "User": "root", "Passwd": "sqldb", "SshPort": "22" }, { "IP": "192.168.20.165", "HostName": "rhel64-test8", "User": "root", "Passwd": "sqldb", "SshPort": "22" },{ "IP": "192.168.20.166", "HostName": "rhel64-test9", "User": "root", "Passwd": "sqldb", "SshPort": "22" } ] } ;

//var SYS_JSON = { "ProgPath": "/opt/sqldb/bin/" } ;

var FILE_NAME_PRE_CHECK_HOST = "preCheckHost.js" ;
var RET_JSON        = new Object() ;
RET_JSON[HostInfo]  = [] ;
var rc              = SDB_OK ;
var errMsg          = "" ;


/* *****************************************************************************
@discretion: push tool programs and js scripts to target host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
@return void
***************************************************************************** */
function _pushPacket( ssh )
{
   var src = "" ;
   var dest = "" ;
   var local_prog_path = "" ;
   var local_spt_path  = "" ;
   
   // tool programs used to start remote sdbcm in remote
   var programs = [ "sdblist", "sdbcmd", "sdbcm", "sdbcmart", "sdbcmtop", "sdb" ] ;

   // js files used to check remote host's info
   var js_files = [ "error.js", "common.js", "define.js", "log.js",
                    "func.js", "checkHostItem.js", "checkHost.js" ] ;
   try
   {
      // 1. get program's path
      try
      {
         local_prog_path = adaptPath( System.getEWD() ) ;
      }
      catch( e )
      {
         SYSEXPHANDLE( e ) ;
         rc = GETLASTERROR() ;
         errMsg = "Failed to get current program's working director in localhost" ;
         PD_LOG( arguments, PDERROR, FILE_NAME_PRE_CHECK_HOST,
                 sprintf( errMsg + ", rc: ?, detail: ?", rc, GETLASTERRMSG() ) ) ;
         exception_handle( rc, errMsg ) ;
      }
      // 2. get js script's path
      try
      {
         local_spt_path  = getSptPath( local_prog_path ) ;
      }
      catch( e )
      {
         SYSEXPHANDLE( e ) ;
         rc = GETLASTERROR() ;
         errMsg = "Failed to get js script file's path in localhost" ;
         PD_LOG( arguments, PDERROR, FILE_NAME_PRE_CHECK_HOST,
                 errMsg + ", rc: " + rc + ", detail: " + GETLASTERRMSG() ) ;
         exception_handle( rc, errMsg ) ;
      }
      PD_LOG( arguments, PDDEBUG, FILE_NAME_PRE_CHECK_HOST,
              sprintf( "local_prog_path is: ?, local_spt_path is: ?",
                       local_prog_path, local_spt_path ) ) ;
      // 3. push programs and js script files to target host  
      if ( SYS_LINUX == SYS_TYPE )
      {
         // push tool programs
         for ( var i = 0; i < programs.length; i++ )
         {
            src = local_prog_path + programs[i] ;
            dest = OMA_PATH_TEMP_BIN_DIR_L + programs[i] ;
            ssh.push( src, dest ) ;
         }
         // push js files
         for ( var i = 0; i < js_files.length; i++ )
         {
            src = local_spt_path + js_files[i] ;
            dest = OMA_PATH_TEMP_SPT_DIR_L + js_files[i] ;
            ssh.push( src, dest ) ;
         }         
/*
         // sdblist
         src = local_prog_path + OMA_PROG_SDBLIST_L ;
         dest = OMA_PATH_TEMP_BIN_DIR_L + OMA_PROG_SDBLIST_L ;
         ssh.push( src, dest ) ;
         // sdbcmtop
         src = local_prog_path + OMA_PROG_SDBCMTOP_L ;
         dest = OMA_PATH_TEMP_BIN_DIR_L + OMA_PROG_SDBCMTOP_L ;
         ssh.push( src, dest ) ;
         // sdbcm
         src = local_prog_path + OMA_PROG_SDBCM_L;
         dest = OMA_PATH_TEMP_BIN_DIR_L + OMA_PROG_SDBCM_L ;
         ssh.push( src, dest ) ;
         // sdbcmd
         src = local_prog_path + OMA_PROG_SDBCMD_L;
         dest = OMA_PATH_TEMP_BIN_DIR_L + OMA_PROG_SDBCMD_L ;
         ssh.push( src, dest ) ;
         // sdbcmart
         src = local_prog_path + OMA_PROG_SDBCMART_L ;
         dest = OMA_PATH_TEMP_BIN_DIR_L + OMA_PROG_SDBCMART_L ;
         ssh.push( src, dest ) ;
         // sdb
         src = local_prog_path + OMA_PROG_SDB ;
         dest = OMA_PATH_TEMP_BIN_DIR_L + OMA_PROG_SDB ;
         ssh.push( src, dest ) ;
         // script error.js
         src = local_spt_path + OMA_FILE_ERROR ;
         dest = OMA_PATH_TEMP_SPT_DIR_L + OMA_FILE_ERROR ;
         ssh.push( src, dest ) ;
         // script common.js
         src = local_spt_path + OMA_FILE_COMMON ;
         dest = OMA_PATH_TEMP_SPT_DIR_L + OMA_FILE_COMMON ;
         ssh.push( src, dest ) ;
         // script define.js
         src = local_spt_path + OMA_FILE_DEFINE ;
         dest = OMA_PATH_TEMP_SPT_DIR_L + OMA_FILE_DEFINE ;
         ssh.push( src, dest ) ;
         // script log.js
         src = local_spt_path + OMA_FILE_LOG ;
         dest = OMA_PATH_TEMP_SPT_DIR_L + OMA_FILE_LOG ;
         ssh.push( src, dest ) ;
         // script func.js
         src = local_spt_path + OMA_FILE_FUNC ;
         dest = OMA_PATH_TEMP_SPT_DIR_L + OMA_FILE_FUNC ;
         ssh.push( src, dest ) ;
         // script checkHostItem.js
         src = local_spt_path + OMA_FILE_CHECK_HOST_ITEM ;
         dest = OMA_PATH_TEMP_SPT_DIR_L + OMA_FILE_CHECK_HOST_ITEM ;
         ssh.push( src, dest ) ;
         // script checkHost.js
         src = local_spt_path + OMA_FILE_CHECK_HOST ;
         dest = OMA_PATH_TEMP_SPT_DIR_L + OMA_FILE_CHECK_HOST ;
         ssh.push( src, dest ) ;
*/
      }
      else
      {
         // TODO:
      }
   }
   catch( e )
   {
      SYSEXPHANDLE( e ) ;
      rc = GETLASTERROR() ;
      errMsg = "Failed to push programs and js files to host[" + ssh.getPeerIP() + "]" ;
      PD_LOG( arguments, PDERROR, FILE_NAME_PRE_CHECK_HOST,
              errMsg + ", rc: " + rc + ", detail: " + GETLASTERRMSG() ) ;
      exception_handle( rc, errMsg ) ;
   }
}

/* *****************************************************************************
@discretion: push other pachet to remote host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
@return void
***************************************************************************** */
/*
function _pushPacket2( ssh )
{
   var src = "" ;
   var dest = "" ;
   try
   {
      if ( SYS_LINUX == SYS_TYPE )
      {
         // sdbcm
         src = local_prog_path + OMA_PROG_SDBCM_L;
         dest = OMA_PATH_TEMP_BIN_DIR_L + OMA_PROG_SDBCM_L ;
         ssh.push( src, dest ) ;
         // sdbcmd
         src = local_prog_path + OMA_PROG_SDBCMD_L;
         dest = OMA_PATH_TEMP_BIN_DIR_L + OMA_PROG_SDBCMD_L ;
         ssh.push( src, dest ) ;
         // sdbcmart
         src = local_prog_path + OMA_PROG_SDBCMART_L ;
         dest = OMA_PATH_TEMP_BIN_DIR_L + OMA_PROG_SDBCMART_L ;
         ssh.push( src, dest ) ;
      }
      else
      {
         // TODO: tanzhaobo
      }
   }
   catch( e )
   {
      SYSEXPHANDLE( e ) ;
      rc = GETLASTERROR() ;
      errMsg = "Failed to push programs and js files to host[" + ssh.getPeerIP() + "]" ;
      PD_LOG( arguments, PDERROR, FILE_NAME_PRE_CHECK_HOST,
              errMsg + ", rc: " + rc + ", detail: " + GETLASTERRMSG() ) ;
      exception_handle( rc, errMsg ) ;
   }
}
*/

/* *****************************************************************************
@discretion: change mode in temporary directory
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
@return void
***************************************************************************** */
function _changeModeInTmpDir( ssh )
{
   var cmd = "" ;
   try
   {
      if ( SYS_LINUX == SYS_TYPE )
      {
        // change mode
        cmd = "chmod -R 755 " + OMA_PATH_TEMP_BIN_DIR_L ;
        ssh.exec( cmd ) ;
      }
      else
      {
         // TODO: tanzhaobo
      }
   }
   catch( e )
   {
      SYSEXPHANDLE( e ) ;
      rc = GETLASTERROR() ;
      errMsg = "Failed to change the newly created temporary directory's mode in host[" + ssh.getPeerIP() + "]" ;
      PD_LOG( arguments, PDERROR, FILE_NAME_PRE_CHECK_HOST,
              errMsg + ", rc: " + rc + ", detail: " + GETLASTERRMSG() ) ;
      exception_handle( rc, errMsg ) ;
   }
}

/* *****************************************************************************
@discretion: start sdbcm program in remote or local host
@author: Tanzhaobo
@parameter
   ssh[object]: ssh object
   port[string]: port for remote sdbcm
@return void
***************************************************************************** */
function _startTmpCM( ssh, port, secs )
{
   var cmd = "" ;
   var cmd1 = "" ;
   var cmd2 = "" ;
   if ( SYS_LINUX == SYS_TYPE )
   {
      try
      {
         cmd1 = "cd " + OMA_PATH_TEMP_BIN_DIR_L ;
         cmd2 = "./" + OMA_PROG_SDBCMART_L ;
         cmd2 += " " + OMA_OPTION_SDBCMART_I ;
         cmd2 += " " + OMA_OPTION_SDBCMART_STANDALONE ;
         cmd2 += " " + OMA_OPTION_SDBCMART_PORT ;
         cmd2 += " " + port ;
         cmd2 += " " + OMA_OPTION_SDBCMART_ALIVETIME ;
         cmd2 += " " + secs ;
         cmd = cmd1 + " ; " + cmd2 ;
println( "start tmp sdbcm execute command: " + cmd ) ;
         ssh.exec( cmd ) ;
      }
      catch ( e )
      {
         SYSEXPHANDLE( e ) ;
         rc = GETLASTERROR() ;
         errMsg = "Failed to start temporary sdbcm in host[" + ssh.getPeerIP() + "]" ;
         PD_LOG( arguments, PDERROR, FILE_NAME_PRE_CHECK_HOST,
                 sprintf( errMsg + ", rc: ?, detail: ?", rc, GETLASTERRMSG() ) ) ;
         exception_handle( rc, errMsg ) ;
      }
      // wait util sdbcm start in target host
      var times = 0 ;
      for ( ; times < OMA_TRY_TIMES; times++ )
      {
         var isRunning = isSdbcmRunning ( ssh ) ;
         if ( isRunning )
         {
            break ;
         }
         else
         {
            sleep( OMA_SLEEP_TIME ) ;
         }
      }
// TODO: println
/*
      if ( OMA_TRY_TIMES <= times )
      {
         errMsg = "Time out, temporary sdbcm does not start successfully in host[" + ssh.getPeerIP() + "]" ;
         PD_LOG( arguments, PDERROR, FILE_NAME_PRE_CHECK_HOST, errMsg ) ;
         exception_handle( SDB_TIMEOUT, errMsg ) ;
      }
*/
   }
   else
   {
      //TODO:
   }
}

/* *****************************************************************************
@discretion: install temporary sdbcm in target host
@author: Tanzhaobo
@parameter
   ssh[object]: the ssh object
   ip[string]: the target ip address
@return
   retObj[string]: the result of install
***************************************************************************** */
function _installTmpCM( ssh, ip )
{
   var retObj = new preCheckResult() ;
   retObj[IP] = ip ;
   
/*
   // test whether sdbcm has been installed in local,
   // if so, no need to push packet 
   var flag = isInLocalHost( ssh ) ;
   if ( flag )
   {
      // get local sdbcm port
      retObj[AgentPort] = "" + getSdbcmPort( ssh ) ;
      return retObj ;
   }
*/

   // 1. build directory in target host
   PD_LOG( arguments, PDDEBUG, FILE_NAME_PRE_CHECK_HOST,
           "create temporary director in host: " + ssh.getPeerIP() ) ;
   createTmpDir( ssh ) ;

   // 2. push tool programs and js files to target host
   PD_LOG( arguments, PDDEBUG, FILE_NAME_PRE_CHECK_HOST,
           "push packet to host: " + ssh.getPeerIP() ) ;
   _pushPacket( ssh ) ;
   
/*
   // push other packet to target host
   PD_LOG( arguments, PDDEBUG, FILE_NAME_PRE_CHECK_HOST,
           "push packet2 to host: " + ssh.getPeerIP() ) ;
   _pushPacket2( ssh ) ;
*/
   // 3. change temporary director's mode
   PD_LOG( arguments, PDDEBUG, FILE_NAME_PRE_CHECK_HOST,
           "change the mode of temporary director in host: " + ssh.getPeerIP() ) ;
   _changeModeInTmpDir( ssh ) ;

/*
   // check whether sdbcm is running in target host
   PD_LOG( arguments, PDDEBUG, FILE_NAME_PRE_CHECK_HOST,
           "check whether sdbcm is running in host: " + ssh.getPeerIP() ) ;
   flag = isSdbcmRunning( ssh ) ;
   if ( flag )
   {
      PD_LOG( arguments, PDDEBUG, FILE_NAME_PRE_CHECK_HOST,
              "sdbcm is  running in host: " + ssh.getPeerIP() ) ;
      retObj[AgentPort] = "" + getSdbcmPort( ssh ) ;
      removeTmpDir( ssh ) ;
      return retObj ;
   }
*/

   // 4. get a usable port in target host for installing temporary sdbcm
   PD_LOG( arguments, PDDEBUG, FILE_NAME_PRE_CHECK_HOST,
           "get a usable port from remote for sdbcm running in host: " + ssh.getPeerIP() ) ;
   var port = getAUsablePortFromRemote( ssh ) ;
   if ( OMA_PORT_INVALID == port )
   {
      errMsg = "Failed to get a usable port in host[" + ssh.getPeerIP() + "]" ;
      PD_LOG( arguments, PDERROR, FILE_NAME_PRE_CHECK_HOST, errMsg ) ;
      exception_handle( SDB_SYS, errMsg ) ;
   }

   // 5. start temporary sdbcm program in target host
   PD_LOG( arguments, PDDEBUG, FILE_NAME_PRE_CHECK_HOST,
           "start temporary sdbcm in host: " + ssh.getPeerIP() ) ;
   _startTmpCM( ssh, port, OMA_TMP_SDBCM_ALIVE_TIME ) ;

   PD_LOG( arguments, PDDEBUG, FILE_NAME_PRE_CHECK_HOST,
           sprintf( "start temporary sdbcm successfully in host ?, the port is: ?",
                     ssh.getPeerIP(), port ) ) ;
   // 6. return the temporary sdbcm to omsvc
   retObj[AgentPort] = port + "" ;
   return retObj ;
}

function main()
{
   PD_LOG( arguments, PDEVENT, FILE_NAME_PRE_CHECK_HOST, "Begin to pre-check host" ) ;
   
   var infoArr = null ;
   var arrLen = null ;

   try
   {
      infoArr = BUS_JSON[HostInfo] ;
      arrLen = infoArr.length ;
//local_prog_path = SYS_JSON[ProgPath] ;
//local_prog_path = System.getEWD() ;
   }
   catch( e )
   {
      SYSEXPHANDLE( e ) ;
      errMsg = "Js receive invalid argument" ;
      rc = GETLASTERROR() ;
      // record error message in log
      PD_LOG( arguments, PDEVENT, FILE_NAME_PRE_CHECK_HOST,
              sprintf( errMsg + ", rc: ?, detail: ?", rc, GETLASTERRMSG() ) ) ;
      // tell to user error happen
      exception_handle( SDB_INVALIDARG, errMsg ) ;
   }
   if ( arrLen == 0 )
   {
      errMsg = "Not specified any host to pre-check" ;
      PD_LOG( arguments, PDEVENT, FILE_NAME_PRE_CHECK_HOST, errMsg ) ;
      exception_handle( SDB_INVALIDARG, errMsg ) ;
   }
   
   for( var i = 0; i < arrLen; i++ )
   {
      var ssh      = null ;
      var obj      = null ;
      var user     = null ;
      var passwd   = null ;
      var ip       = null ;
      var sshport  = null ;
      var ret      = new preCheckResult() ;
      try
      {
         obj       = infoArr[i] ;
         ip        = obj[IP] ;
         user      = obj[User] ;
         passwd    = obj[Passwd] ;
         sshport   = parseInt(obj[SshPort]) ;
         ret       = new preCheckResult() ;
         
         ssh = new Ssh( ip, user, passwd, sshport ) ;
         // install
         ret = _installTmpCM( ssh, ip ) ;
      }
      catch ( e )
      {
         SYSEXPHANDLE( e ) ;
         ret[IP] = ip ;
         ret[Errno] = GETLASTERROR() ;
         ret[Detail] = GETLASTERRMSG() ;
         PD_LOG( arguments, PDERROR, FILE_NAME_PRE_CHECK_HOST,
                 sprintf("Failed to pre-check host[?], rc: ?, detail: ?", ip, ret[Errno], ret[Detail] ) ) ;
      }
      // set return result
      RET_JSON[HostInfo].push( ret ) ;
   }

   PD_LOG( arguments, PDEVENT, FILE_NAME_PRE_CHECK_HOST, "Finish pre-checking host" ) ;
println("RET_JSON is: " + JSON.stringify(RET_JSON)) ;
   return RET_JSON ;
}

// execute
main() ;

