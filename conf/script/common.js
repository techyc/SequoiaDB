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
@description: Js class for the js files in current document
@modify list:
   2014-7-26 Zhaobo Tan  Init
*/

function scanHostResult()
{
   this.errno                     = SDB_OK ;
   this.detail                    = "" ;
   this.Status                    = "" ;
   this.IP                        = "" ;
   this.HostName                  = "" ;
}

function preCheckResult()
{
   this.errno                     = SDB_OK ;
   this.detail                    = "" ;
   this.IP                        = "" ;
   this.AgentService              = "" ;
}

function postCheckResult()
{
   this.errno                     = SDB_OK ;
   this.detail                    = "" ;
   this.IP                        = "" ;
}

function addHostResult()
{
   this.errno                     = SDB_OK ;
   this.detail                    = "" ;
   this.IP                        = "" ;
   //this.HasInstall                = false ;
}

function checkAddHostInfoResult()
{
   this.errno                     = SDB_OK ;
   this.detail                    = "" ;
}

function addHostRollbackResult()
{
   this.errno                     = SDB_OK ;
   this.detail                    = "" ;
   this.IP                        = "" ;
   this.HasUninstall              = false ;
}

function installNodeResult()
{
   this.errno                     = SDB_OK ;
   this.detail                    = "" ;
}

function rollbackNodeResult()
{
   this.errno                     = SDB_OK ;
   this.detail                    = "" ;
}

function removeHostResult()
{
   this.errno                     = SDB_OK ;
   this.detail                    = "" ;
   this.IP                        = "" ;
   this.HasUninstall              = false ;
}
