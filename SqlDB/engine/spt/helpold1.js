var help = function() {
   println("   var db = new Sdb()                     connect to datebase use default host 'localhost' and default port 11810");
   println("   var db = new Sdb('localhost', 11810)   connect to database use specified host and port");
   println("   db.help()                              help on db methods");
   println("   db.cs.help()                           help on collection space cs");
   println("   db.cs.cl                               access collection cl on collection space cs");
   println("   db.cs.cl.help()                        help on collection cl");
   println("   db.cs.cl.find()                        list all records");
   println("   db.cs.cl.find({a:1})                   list records where a=1");
   println("   print(x), println(x)                   print out x");
   println("   traceFmt(<type>,<in>,<out>)            format trace input(in) to output(out) by type");
   println("   getErr(ret)                            print error description for return code");
   println("   clear                                  clear the terminal screen");
   println("   history -c                             clear the history");
   println("   quit                                   exit");
}

Sdb.prototype.help = function() {
   println("DB methods:");
   println("   backupOffline([options])");
   println("   cancelTask(<id>,[isAsync])");
   println("   close()");
   println("   createCS(<name>, [pageSize])");
   println("   createCataRG(<host>,<service>,<dbpath>,[config])");
   println("   createProcedures(<code>)");
   println("   createRG(<name>)");
   println("   createUsr(<name>, <password>)");
   println("   dropCS(<name>)");
   println("   dropUsr(<name>, <password>)");
   println("   eval(<code>)");
   println("   exec(<select sql>)");
   println("   execUpdate(<other sql>)");
   println("   flushConfigure(<rule>)");
   println("   getCS(<name>)");
   println("   getRG(<name>|<id>)");
   println("   list(<listType>, [cond], [sel], [sort])");
   println("   listBackup([options], [cond], [sel])");
   println("   listCollectionSpaces()");
   println("   listCollections()");
   println("   listProcedures([condition])");
   println("   listReplicaGroups()");
   println("   listTasks([cond],[sel],[orderBy],[hint])");
   println("   removeBackup([options])");
   println("   removeProcedures(<function name>)");
   println("   removeRG(<name>)");
   println("   resetSnapshot([cond])");
   println("   setSessionAttr(<options>) - # eg:{\"PreferedInstance\":\"m\"/\"M\"/\"s\"/\"S\"/\"a\"/\"A\"/1-7}, prefer to choose master/slave/anyone/node1-node7");
   println("   snapshot(<snapType>, [cond], [sel], [sort])");
   println("   startRG(<name1>,[name2]...)");
   println("   traceOff([dump file])");
   println("   traceResume()");   
   println("   traceStatus()");
   println("   transBegin()");
   println("   transCommit()");
   println("   transRollback()"); 
   println("   traceOn(<bufferSize>,[options])");
   println("   waitTasks(<id1>,[id2],...)");   
}

SdbNode.prototype.help = function() {
   println("Node methods:");
   println("   connect()");
   println("   getHostName()");
   println("   getNodeDetail()");
   println("   getServiceName()");
   println("   start()");
   println("   stop()");
}

SdbReplicaGroup.prototype.help = function() {
   println("Replica group methods:");
   println("   createNode(<host>,<service>,<dbpath>,[config])");
   println("   getDetail()");
   println("   getMaster()");
   println("   getNode(<nodename>|<hostname>,<servicename>)");
   println("   getSlave()");
   println("   removeNode(<host>,<service>,[config])");
   println("   start()");
   println("   stop()");
}

SdbCS.prototype.help = function() {
   println("Collection Space methods:");
   println("   createCL(<name>, [options])");
   println("   dropCL(<name>)");
   println("   getCL(<name>)");
}

SdbCollection.prototype.help = function() {
   println("Collection methods:");
   println("   aggregate(< project | match | limit | skip | group | sort >...)");
   println("   attachCL(<subCLFullName>, <options>)");
   println("   count([cond])");
   println("   createIndex(<name>, <indexDef>, [isUnique])");
   println("   detachCL(<subCLFullName>)");
   println("   dropIndex(<name>)");
   println("   find([cond], [sel])");
   println("   getIndex(<name>)");
   println("   insert(<doc> or <docs>, [flags])");
   println("   listIndexes()");
   println("   remove([cond], [hint])");
   println("   split(<source group>,<target group>,<percent(0~100)|<condition>, [endcondition])");
   println("   splitAsync(<source group>,<target group>,<percent(0~100)|<condition>, [endcondition])");
   println("   update(<rule>, [cond], [hint])");
   println("   upsert(<rule>, [cond], [hint])");
   //println("   rename(<newName>)");
   //println("   alter(<options>)");
}

SdbQuery.prototype.help = function() {
   println("find() modifiers:");
   println("   sort(<sort>)");
   println("   hint(<hint>)");
   println("   limit(<num>)");
   println("   skip(<num>)");
   println("find() cursor methods:");
   println("   current()");
   //println("   deleteCurrent()");
   println("   next()");
   println("   close()");
   //println("   updateCurrent(<rule>)");
   println("find() methods:");
   println("   count() - # of records matching query, ignores skip, limit");
   println("   size() - # of records matching query, hornors skip, limit");
   println("   toArray()");
   println("   [i] - use array index to access cursor");
}

SdbCursor.prototype.help = function() {
   println("Cursor methods:");
   println("   current()");
   println("   deleteCurrent()");
   println("   next()");
   println("   updateCurrent(<rule>)");
}

Bson.prototype.help = function() {
   println("Bson methods:");
   println("   toObj() - convert to javascript object");
   println("   toJson() - convert to json string");
}

CLCount.prototype.help = function() {
   println("count() modifiers:");
   println("   hint(<hint>)");
}

