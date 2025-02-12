#include "pysqldb.hpp"
#include "ossUtil.hpp"
#include "ossVer.hpp"
#include "client.hpp"

using namespace sdbclient;

__METHOD_IMP(sdb_create_client)
{
   sdb *client = NULL;
   NEW_CPPOBJECT( client, sdb ) ;
   if ( NULL == client )
   {
      return NULL ;
   }

   return MAKE_PYOBJECT( client ) ;
}

__METHOD_IMP(sdb_release_client)
{
   INT32 rc      = 0 ;
   PYOBJECT *obj = NULL ;
   sdb *client   = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   DELETE_CPPOBJECT( client ) ;

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_connect)
{
   INT32 rc            = 0 ;
   PYOBJECT *obj       = NULL ;
   sdb *client         = NULL ;
   const CHAR *host    = NULL ;
   const CHAR *service = NULL ;
   const CHAR *user    = NULL ;
   const CHAR *psw     = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "Ossss", &obj, &host, &service, &user, &psw ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;

   rc = client->connect( host, service, user, psw ) ;

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_disconnect)
{
   INT32 rc      = 0 ;
   PYOBJECT *obj = NULL ;
   sdb *client   = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;

   client->disconnect() ;

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_create_user)
{
   INT32 rc              = 0 ;
   PYOBJECT *obj         = NULL ;
   sdb *client           = NULL ;
   const CHAR *user_name = NULL ;
   const CHAR *psw       = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "Oss", &obj, &user_name, &psw ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;

   rc = client->createUsr( user_name, psw ) ;

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_remove_user)
{
   INT32 rc              = 0 ;
   PYOBJECT *obj         = NULL ;
   sdb *client           = NULL ;
   const CHAR *user_name = NULL ;
   const CHAR *psw       = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "Oss", &obj, &user_name, &psw ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;

   rc = client->removeUsr( user_name, psw ) ;

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_get_snapshot)
{
   INT32 rc                       = 0 ;
   INT32 snap_type                = 0 ;
   PYOBJECT *obj                  = NULL ;
   PYOBJECT *cursor_obj           = NULL ;
   PYOBJECT *bson_condition       = NULL ;
   PYOBJECT *bson_selector        = NULL ;
   PYOBJECT *bson_order_by        = NULL ;
   sdb *client                    = NULL ;
   sdbCursor *cursor              = NULL ;
   const bson::BSONObj *condition = NULL ;
   const bson::BSONObj *selector  = NULL ;
   const bson::BSONObj *order_by  = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OOiOOO", &obj, &cursor_obj, &snap_type,
      &bson_condition, &bson_selector, &bson_order_by ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYOBJECT_TO_COBJECT( cursor_obj, sdbCursor, cursor ) ;
   CAST_PYBSON_TO_CPPBSON( bson_condition, condition ) ;
   CAST_PYBSON_TO_CPPBSON( bson_selector, selector ) ;
   CAST_PYBSON_TO_CPPBSON( bson_order_by, order_by ) ;

   rc = client->getSnapshot( *cursor, snap_type, *condition,
      *selector, *order_by ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( condition ) ;
   DELETE_CPPOBJECT( selector ) ;
   DELETE_CPPOBJECT( order_by ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_reset_snapshot)
{
   INT32 rc                       = 0 ;
   PYOBJECT *obj                  = NULL ;
   PYOBJECT *bson_condition       = NULL ;
   sdb *client                    = NULL ;
   const bson::BSONObj *condition = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OO", &obj, &bson_condition ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYBSON_TO_CPPBSON( bson_condition, condition ) ;

   client->resetSnapshot( *condition ) ;

done:
   DELETE_CPPOBJECT( condition ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_get_list)
{
   INT32 rc                       = 0 ;
   INT32 list_type                = 0 ;
   PYOBJECT *obj                  = NULL ;
   PYOBJECT *cursor_obj           = NULL ;
   PYOBJECT *bson_condition       = NULL ;
   PYOBJECT *bson_selector        = NULL ;
   PYOBJECT *bson_order_by        = NULL ;
   sdb *client                    = NULL ;
   sdbCursor *cursor              = NULL ;
   const bson::BSONObj *condition = NULL ;
   const bson::BSONObj *selector  = NULL ;
   const bson::BSONObj *order_by  = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OOiOOO", &obj, &cursor_obj, &list_type,
      &bson_condition, &bson_selector, &bson_order_by ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYOBJECT_TO_COBJECT( cursor_obj, sdbCursor, cursor ) ;
   CAST_PYBSON_TO_CPPBSON( bson_condition, condition ) ;
   CAST_PYBSON_TO_CPPBSON( bson_selector, selector ) ;
   CAST_PYBSON_TO_CPPBSON( bson_order_by, order_by ) ;

   rc = client->getList( *cursor, list_type, *condition, *selector, *order_by ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( condition ) ;
   DELETE_CPPOBJECT( selector ) ;
   DELETE_CPPOBJECT( order_by ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_get_collection_space)
{
   INT32 rc               = 0 ;
   PYOBJECT *obj          = NULL ;
   PYOBJECT *cs_obj       = NULL ;
   const CHAR *cs_name    = NULL ;
   sdb *client            = NULL ;
   sdbCollectionSpace *cs = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OsO", &obj, &cs_name, &cs_obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYOBJECT_TO_COBJECT( cs_obj, sdbCollectionSpace, cs ) ;

   rc = client->getCollectionSpace( cs_name, *cs ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_get_collection)
{
   INT32 rc               = 0 ;
   PYOBJECT *obj          = NULL ;
   PYOBJECT *cl_obj       = NULL ;
   const CHAR *cl_name    = NULL ;
   sdb *client            = NULL ;
   sdbCollection *cl      = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OsO", &obj, &cl_name, &cl_obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYOBJECT_TO_COBJECT( cl_obj, sdbCollection, cl ) ;

   rc = client->getCollection( cl_name, *cl ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_create_collection_space)
{
   INT32 rc               = 0 ;
   INT32 page_size        = 0 ;
   PYOBJECT *obj          = NULL ;
   PYOBJECT *cs_obj       = NULL ;
   const CHAR *cs_name    = NULL ;
   sdb *client            = NULL ;
   sdbCollectionSpace *cs = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OsiO", &obj, &cs_name, &page_size,
      &cs_obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYOBJECT_TO_COBJECT( cs_obj, sdbCollectionSpace, cs ) ;

   rc = client->createCollectionSpace( cs_name, page_size, *cs ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_drop_collection_space)
{
   INT32 rc            = 0 ;
   PYOBJECT *obj       = NULL ;
   const CHAR *cs_name = NULL ;
   sdb *client         = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "Os", &obj, &cs_name ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;

   rc = client->dropCollectionSpace( cs_name ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_list_collection_spaces)
{
   INT32 rc             = 0 ;
   PYOBJECT *obj        = NULL ;
   PYOBJECT *cursor_obj = NULL ;
   sdb *client          = NULL ;
   sdbCursor *cursor    = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OO", &obj, &cursor_obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYOBJECT_TO_COBJECT( cursor_obj, sdbCursor, cursor ) ;

   rc = client->listCollectionSpaces( *cursor ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_list_collections)
{
   INT32 rc             = 0 ;
   PYOBJECT *obj        = NULL ;
   PYOBJECT *cursor_obj = NULL ;
   sdb *client          = NULL ;
   sdbCursor *cursor    = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OO", &obj, &cursor_obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYOBJECT_TO_COBJECT( cursor_obj, sdbCursor, cursor ) ;

   rc = client->listCollections( *cursor ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_list_replica_groups)
{
   INT32 rc             = 0 ;
   PYOBJECT *obj        = NULL ;
   PYOBJECT *cursor_obj = NULL ;
   sdb *client          = NULL ;
   sdbCursor *cursor    = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OO", &obj, &cursor_obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYOBJECT_TO_COBJECT( cursor_obj, sdbCursor, cursor ) ;

   rc = client->listReplicaGroups( *cursor ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_get_replica_group_by_name)
{
   INT32 rc               = 0 ;
   PYOBJECT *obj          = NULL ;
   PYOBJECT *group_obj    = NULL ;
   sdb *client            = NULL ;
   sdbReplicaGroup *group = NULL ;
   const CHAR *group_name = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OsO", &obj, &group_name, &group_obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYOBJECT_TO_COBJECT( group_obj, sdbReplicaGroup, group ) ;

   rc = client->getReplicaGroup( group_name, *group ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_get_replica_group_by_id)
{
   INT32 rc               = 0 ;
   INT32 group_id         = 0 ;
   PYOBJECT *obj          = NULL ;
   PYOBJECT *group_obj    = NULL ;
   sdb *client            = NULL ;
   sdbReplicaGroup *group = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OiO", &obj, &group_id, &group_obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYOBJECT_TO_COBJECT( group_obj, sdbReplicaGroup, group ) ;

   rc = client->getReplicaGroup( group_id, *group ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_create_replica_group)
{
   INT32 rc               = 0 ;
   PYOBJECT *obj          = NULL ;
   PYOBJECT *group_obj    = NULL ;
   sdb *client            = NULL ;
   sdbReplicaGroup *group = NULL ;
   const CHAR *group_name = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OsO", &obj, &group_name, &group_obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYOBJECT_TO_COBJECT( group_obj, sdbReplicaGroup, group ) ;
   rc = client->createReplicaGroup( group_name, *group ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_remove_replica_group)
{
   INT32 rc               = 0 ;
   PYOBJECT *obj          = NULL ;
   sdb *client            = NULL ;
   const CHAR *group_name = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "Os", &obj, &group_name ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;

   rc = client->removeReplicaGroup( group_name ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_create_replica_cata_group)
{
   INT32 rc                       = 0 ;
   PYOBJECT *obj                  = NULL ;
   PYOBJECT *bson_configure       = NULL ;
   sdb *client                    = NULL ;
   const CHAR *host               = NULL ;
   const CHAR *service            = NULL ;
   const CHAR *db_path            = NULL ;
   const bson::BSONObj *configure = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OsssO", &obj, &host, &service, &db_path,
      &bson_configure ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYBSON_TO_CPPBSON( bson_configure, configure ) ;

   rc = client->createReplicaCataGroup( host, service, db_path, *configure ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( configure ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_exec_update)
{
   INT32 rc        = 0 ;
   PYOBJECT *obj   = NULL ;
   sdb *client     = NULL ;
   const CHAR *sql = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "Os", &obj, &sql ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;

   rc = client->execUpdate( sql ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_exec_sql)
{
   INT32 rc             = 0 ;
   PYOBJECT *obj        = NULL ;
   PYOBJECT *cursor_obj = NULL ;
   sdb *client          = NULL ;
   sdbCursor *cursor    = NULL ;
   const CHAR *sql      = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OsO", &obj, &sql, &cursor_obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYOBJECT_TO_COBJECT( cursor_obj, sdbCursor, cursor ) ;

   rc = client->exec( sql, *cursor ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_transaction_begin)
{
   INT32 rc      = 0 ;
   PYOBJECT *obj = NULL ;
   sdb *client   = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;

   rc = client->transactionBegin() ;

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_transaction_commit)
{
   INT32 rc      = 0 ;
   PYOBJECT *obj = NULL ;
   sdb *client   = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;

   rc = client->transactionCommit() ;

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_transaction_rollback)
{
   INT32 rc      = 0 ;
   PYOBJECT *obj = NULL ;
   sdb *client   = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;

   rc = client->transactionRollback() ;

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_flush_configure)
{
   INT32 rc                    = 0 ;
   PYOBJECT *obj               = NULL ;
   PYOBJECT *bson_option       = NULL ;
   sdb *client                 = NULL ;
   const bson::BSONObj *option = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OO", &obj, &bson_option ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYBSON_TO_CPPBSON( bson_option, option ) ;

   rc = client->flushConfigure( *option ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( option ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_create_JS_procedure)
{
   INT32 rc              = 0 ;
   PYOBJECT *obj         = NULL ;
   sdb *client           = NULL ;
   const CHAR *str_code  = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "Os", &obj, &str_code ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;

   rc = client->crtJSProcedure( str_code ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_remove_procedure)
{
   INT32 rc              = 0 ;
   PYOBJECT *obj         = NULL ;
   sdb *client           = NULL ;
   const CHAR *spname  = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "Os", &obj, &spname ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;

   rc = client->rmProcedure( spname ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_list_procedures)
{
   INT32 rc                       = 0 ;
   PYOBJECT *obj                  = NULL ;
   PYOBJECT *cursor_object        = NULL ;
   PYOBJECT *bson_condition       = NULL ;
   sdb *client                    = NULL ;
   sdbCursor *cursor              = NULL ;
   const bson::BSONObj *condition = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OOO", &obj,
      &cursor_object, &bson_condition ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYOBJECT_TO_COBJECT( cursor_object, sdbCursor, cursor ) ;
   CAST_PYBSON_TO_CPPBSON( bson_condition, condition ) ;

   rc = client->listProcedures( *cursor, *condition ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( condition ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_eval_JS)
{
   INT32 rc                          = 0 ;
   SDB_SPD_RES_TYPE sdb_spd_res_type = SDB_SPD_RES_TYPE_VOID ;
   PYOBJECT *obj                     = NULL ;
   PYOBJECT *cursor_object           = NULL ;
   sdb *client                       = NULL ;
   sdbCursor *cursor                 = NULL ;
   const CHAR *code                  = NULL ;
   const bson::BSONObj errmsg;

   if ( !PARSE_PYTHON_ARGS( args, "OOs", &obj, &cursor_object, &code ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYOBJECT_TO_COBJECT( cursor_object, sdbCursor, cursor ) ;

   rc = client->evalJS( *cursor, code, &sdb_spd_res_type, errmsg ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_backup_offline)
{
   INT32 rc                    = 0 ;
   PYOBJECT *obj               = NULL ;
   PYOBJECT *bson_option       = NULL ;
   sdb *client                 = NULL ;
   const bson::BSONObj *option = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OO", &obj, &bson_option ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYBSON_TO_CPPBSON( bson_option, option ) ;

   rc = client->backupOffline( *option ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( option ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_list_backup)
{
   INT32 rc                       = 0 ;
   PYOBJECT *obj                  = NULL ;
   PYOBJECT *cursor_obj           = NULL ;
   PYOBJECT *bson_option          = NULL ;
   PYOBJECT *bson_condition       = NULL ;
   PYOBJECT *bson_selector        = NULL ;
   PYOBJECT *bson_order_by        = NULL ;
   sdb *client                    = NULL ;
   sdbCursor *cursor              = NULL ;
   const bson::BSONObj *option    = NULL ;
   const bson::BSONObj *condition = NULL ;
   const bson::BSONObj *selector  = NULL ;
   const bson::BSONObj *order_by  = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OOOOOO", &obj, &cursor_obj, &bson_option,
      &bson_condition, &bson_selector, &bson_order_by) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYOBJECT_TO_COBJECT( cursor_obj, sdbCursor, cursor ) ;
   CAST_PYBSON_TO_CPPBSON( bson_option, option ) ;
   CAST_PYBSON_TO_CPPBSON( bson_condition, condition ) ;
   CAST_PYBSON_TO_CPPBSON( bson_selector, selector ) ;
   CAST_PYBSON_TO_CPPBSON( bson_order_by, order_by ) ;

   rc = client->listBackup( *cursor, *option, *condition, *selector, *order_by ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( option ) ;
   DELETE_CPPOBJECT( condition ) ;
   DELETE_CPPOBJECT( selector ) ;
   DELETE_CPPOBJECT( order_by ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_remove_backup)
{
   INT32 rc                    = 0 ;
   PYOBJECT *obj               = NULL ;
   PYOBJECT *bson_option       = NULL ;
   sdb *client                 = NULL ;
   const bson::BSONObj *option = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OO", &obj, &bson_option ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYBSON_TO_CPPBSON( bson_option, option ) ;

   rc = client->removeBackup( *option ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( option ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_list_tasks)
{
   INT32 rc                       = 0 ;
   PYOBJECT *obj                  = NULL ;
   PYOBJECT *cursor_obj           = NULL ;
   PYOBJECT *bson_condition       = NULL ;
   PYOBJECT *bson_selector        = NULL ;
   PYOBJECT *bson_order_by        = NULL ;
   PYOBJECT *bson_hint            = NULL ;
   sdb *client                    = NULL ;
   sdbCursor *cursor              = NULL ;
   const bson::BSONObj *condition = NULL ;
   const bson::BSONObj *selector  = NULL ;
   const bson::BSONObj *order_by  = NULL ;
   const bson::BSONObj *hint      = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OOOOOO", &obj, &cursor_obj, &bson_condition,
      &bson_selector, &bson_order_by, &bson_hint) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYOBJECT_TO_COBJECT( cursor_obj, sdbCursor, cursor ) ;
   CAST_PYBSON_TO_CPPBSON( bson_condition, condition ) ;
   CAST_PYBSON_TO_CPPBSON( bson_selector, selector ) ;
   CAST_PYBSON_TO_CPPBSON( bson_order_by, order_by ) ;
   CAST_PYBSON_TO_CPPBSON( bson_hint, hint ) ;

   rc = client->listTasks( *cursor, *condition, *selector, *order_by, *hint ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( condition ) ;
   DELETE_CPPOBJECT( selector ) ;
   DELETE_CPPOBJECT( order_by ) ;
   DELETE_CPPOBJECT( hint ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_wait_task)
{
   INT32 rc               = 0 ;
   PYOBJECT *obj          = NULL ;
   PYOBJECT *task_ids_obj = NULL ;
   sdb *client            = NULL ;
   SINT64 *task_ids       = NULL ;
   SINT32 num             = 0 ;

   if ( !PARSE_PYTHON_ARGS( args, "OOi", &obj, &task_ids_obj, &num ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   task_ids = new SINT64[num] ;
   MAKE_PYLIST_TO_BUFFER( task_ids_obj, task_ids) ;
   rc = client->waitTasks( task_ids, num ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   if ( NULL != task_ids )
   {
      delete [] task_ids ;
   }
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_cancel_task)
{
   INT32 rc         = 0 ;
   PYOBJECT *obj    = NULL ;
   sdb *client      = NULL ;
   SINT64 task_id   = 0 ;
   BOOLEAN is_async = 0 ;

   if ( !PARSE_PYTHON_ARGS( args, "OLi", &obj, &task_id, &is_async ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;

   rc = client->cancelTask( task_id, is_async ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_set_session_attri)
{
   INT32 rc                    = 0 ;
   PYOBJECT *obj               = NULL ;
   PYOBJECT *bson_option       = NULL ;
   sdb *client                 = NULL ;
   const bson::BSONObj *option = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OO", &obj, &bson_option ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   CAST_PYBSON_TO_CPPBSON( bson_option, option ) ;

   rc = client->setSessionAttr( *option ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( option ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_close_all_cursors)
{
   INT32 rc      = 0 ;
   PYOBJECT *obj = NULL ;
   sdb *client   = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;

   rc = client->closeAllCursors() ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(sdb_is_valid)
{
   INT32 rc        = 0 ;
   INT32 result    = FALSE ;
   PYOBJECT *obj   = NULL ;
   sdb *client     = NULL ;
   BOOLEAN isvalid = FALSE;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdb, client ) ;
   rc = client->isValid( &isvalid ) ;
   if ( rc )
   {
      goto done ;
   }

   if ( isvalid )
   {
      result = 1 ;
   }
   else
   {
      result = 0 ;
   }
done:
   return MAKE_RETURN_INT_INT( rc, result ) ;
}

__METHOD_IMP(sdb_get_version)
{
   int version = 0 ;
   int sub_version = 0 ;
   int release = 0 ;
   const CHAR *build = NULL ;

   ossGetVersion( &version, &sub_version, &release, &build ) ;

   return MAKE_RETURN_INT_INT_INT_STRING( version, sub_version, release, build ) ;
}

__METHOD_IMP(create_cs)
{
   sdbCollectionSpace *cs = NULL ;
   NEW_CPPOBJECT( cs, sdbCollectionSpace ) ;
   if ( NULL == cs )
   {
      return NULL ;
   }

   return MAKE_PYOBJECT( cs ) ;
}

__METHOD_IMP(release_cs)
{
   INT32 rc               = 0 ;
   PYOBJECT *obj          = NULL ;
   sdbCollectionSpace *cs = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollectionSpace, cs ) ;
   DELETE_CPPOBJECT( cs );

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cs_get_collection)
{
   INT32 rc               = 0 ;
   PYOBJECT *obj          = NULL ;
   PYOBJECT *cl_object    = NULL ;
   const CHAR *cl_name    = NULL ;
   sdbCollectionSpace *cs = NULL ;
   sdbCollection *cl      = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OsO", &obj, &cl_name, &cl_object ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollectionSpace, cs ) ;
   CAST_PYOBJECT_TO_COBJECT( cl_object, sdbCollection, cl ) ;

   rc = cs->getCollection( cl_name, *cl ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cs_create_collection)
{
   INT32 rc               = 0 ;
   PYOBJECT *obj          = NULL ;
   PYOBJECT *cl_object    = NULL ;
   const CHAR *cl_name    = NULL ;
   sdbCollectionSpace *cs = NULL ;
   sdbCollection *cl      = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OsO", &obj, &cl_name, &cl_object ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollectionSpace, cs ) ;
   CAST_PYOBJECT_TO_COBJECT( cl_object, sdbCollection, cl ) ;

   rc = cs->createCollection( cl_name, *cl ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cs_create_collection_use_opt)
{
   INT32 rc                    = 0 ;
   PYOBJECT *obj               = NULL ;
   PYOBJECT *cl_object         = NULL ;
   PYOBJECT *bson_option       = NULL ;
   const CHAR *cl_name         = NULL ;
   sdbCollectionSpace *cs      = NULL ;
   sdbCollection *cl           = NULL ;
   const bson::BSONObj *option = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OsOO", &obj, &cl_name, &bson_option,
      &cl_object ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollectionSpace, cs ) ;
   CAST_PYOBJECT_TO_COBJECT( cl_object, sdbCollection, cl ) ;
   CAST_PYBSON_TO_CPPBSON( bson_option, option ) ;

   rc = cs->createCollection( cl_name, *option, *cl ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( option ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cs_drop_collection)
{
   INT32 rc               = 0 ;
   PYOBJECT *obj          = NULL ;
   const CHAR *cl_name    = NULL ;
   sdbCollectionSpace *cs = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "Os", &obj, &cl_name ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollectionSpace, cs ) ;

   rc = cs->dropCollection( cl_name ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cs_get_collection_space_name)
{
   INT32 rc               = 0 ;
   PYOBJECT *obj          = NULL ;
   const CHAR *cs_name    = NULL ;
   sdbCollectionSpace *cs = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollectionSpace, cs ) ;

   cs_name = cs->getCSName() ;

done :
   return MAKE_RETURN_INT_PYSTRING( rc, cs_name ) ;
}

__METHOD_IMP(create_cl) 
{
   sdbCollection *cl = NULL;
   NEW_CPPOBJECT( cl, sdbCollection ) ;
   if ( NULL == cl )
   {
      return NULL ;
   }

   return MAKE_PYOBJECT( cl ) ;
}

__METHOD_IMP(release_cl)
{
   INT32 rc          = 0 ;
   PYOBJECT *obj     = NULL ;
   sdbCollection *cl = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   DELETE_CPPOBJECT( cl ) ;

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cl_get_count)
{
   INT32 rc                       = 0 ;
   SINT64 count                   = 0 ;
   PYOBJECT *obj                  = NULL ;
   PYOBJECT *bson_condition       = NULL ;
   const bson::BSONObj *condition = NULL ;
   sdbCollection *cl              = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OO", &obj, &bson_condition ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   CAST_PYBSON_TO_CPPBSON( bson_condition, condition ) ;

   rc = cl->getCount( count, *condition ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT_LONG( rc, count ) ;
}

__METHOD_IMP(cl_split_by_condition)
{
   INT32 rc                           = 0 ;
   PYOBJECT *obj                      = NULL ;
   PYOBJECT *bson_condition           = NULL ;
   PYOBJECT *bson_end_condition       = NULL ;
   const CHAR *src_name               = NULL ;
   const CHAR *dst_name               = NULL ;
   sdbCollection *cl                  = NULL ;
   const bson::BSONObj *condition     = NULL ;
   const bson::BSONObj *end_condition = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OssOO", &obj, &src_name, &dst_name,
      &bson_condition, &bson_end_condition ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   CAST_PYBSON_TO_CPPBSON( bson_condition, condition ) ;
   CAST_PYBSON_TO_CPPBSON( bson_end_condition, end_condition ) ;

   rc = cl->split( src_name, dst_name, *condition, *end_condition ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( condition ) ;
   DELETE_CPPOBJECT( end_condition ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cl_split_by_percent)
{
   INT32 rc             = 0 ;
   PYOBJECT *obj        = NULL ;
   const CHAR *dst_name = NULL ;
   const CHAR *src_name = NULL ;
   sdbCollection *cl    = NULL ;
   FLOAT64 percent      = 0 ;

   if ( !PARSE_PYTHON_ARGS( args, "Ossd", &obj, &src_name, &dst_name,
      &percent ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;

   rc = cl->split( src_name, dst_name, percent ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cl_split_async_by_condition)
{
   INT32 rc                           = 0 ;
   PYOBJECT *obj                      = NULL ;
   PYOBJECT *bson_condition           = NULL ;
   PYOBJECT *bson_end_condition       = NULL ;
   const CHAR *src_name               = NULL ;
   const CHAR *dst_name               = NULL ;
   sdbCollection *cl                  = NULL ;
   const bson::BSONObj *condition     = NULL ;
   const bson::BSONObj *end_condition = NULL ;
   SINT64 task_id            = 0 ;

   if ( !PARSE_PYTHON_ARGS( args, "OssOO", &obj, &src_name, &dst_name,
      &bson_condition, &bson_end_condition ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   CAST_PYBSON_TO_CPPBSON( bson_condition, condition ) ;
   CAST_PYBSON_TO_CPPBSON( bson_end_condition, end_condition ) ;

   rc = cl->splitAsync( task_id, src_name, dst_name,
      *condition, *end_condition ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( condition ) ;
   DELETE_CPPOBJECT( end_condition ) ;
   return MAKE_RETURN_INT_LONG( rc, task_id ) ;
}

__METHOD_IMP(cl_splite_async_by_percent)
{
   INT32 rc             = 0 ;
   PYOBJECT *obj        = NULL ;
   const CHAR *src_name = NULL ;
   const CHAR *dst_name = NULL ;
   sdbCollection *cl    = NULL ;
   FLOAT64 percent      = 0 ;
   SINT64 task_id       = 0 ;

   if ( !PARSE_PYTHON_ARGS( args, "Ossd", &obj, &src_name, &dst_name,
      &percent ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;

   rc = cl->splitAsync( src_name, dst_name, percent, task_id ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT_LONG( rc, task_id ) ;
}

__METHOD_IMP(cl_bulk_insert)
{
   INT32 rc              = 0 ;
   SINT32 flags          = 0 ;
   PYOBJECT *obj         = NULL ;
   PYOBJECT *list_object = NULL ;
   sdbCollection *cl     = NULL ;

   std::vector< bson::BSONObj > vec_bson ;

   if ( !PARSE_PYTHON_ARGS( args, "OiO", &obj, &flags, &list_object ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   MAKE_PYLIST_TO_VECTOR( list_object, vec_bson ) ;
   rc = cl->bulkInsert( flags, vec_bson ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cl_insert)
{
   INT32 rc                    = 0 ;
   PYOBJECT *obj               = NULL ;
   PYOBJECT *bson_object       = NULL ;
   sdbCollection *cl           = NULL ;
   const bson::BSONObj *object = NULL ;
   bson::OID id ;
   if ( !PARSE_PYTHON_ARGS( args, "OO", &obj, &bson_object ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   CAST_PYBSON_TO_CPPBSON( bson_object, object ) ;
   rc = cl->insert( *object, &id ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( object ) ;
   return MAKE_RETURN_INT_PYSTRING( rc, id.toString().c_str() ) ;
}

__METHOD_IMP(cl_update)
{
   INT32 rc                       = 0 ;
   PYOBJECT *obj                  = NULL ;
   PYOBJECT *bson_rule            = NULL ;
   PYOBJECT *bson_condition       = NULL ;
   PYOBJECT *bson_hint            = NULL ;
   sdbCollection *cl              = NULL ;
   const bson::BSONObj *rule      = NULL ;
   const bson::BSONObj *condition = NULL ;
   const bson::BSONObj *hint      = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OOOO", &obj, &bson_rule,
      &bson_condition, &bson_hint ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   CAST_PYBSON_TO_CPPBSON( bson_rule, rule ) ;
   CAST_PYBSON_TO_CPPBSON( bson_condition, condition ) ;
   CAST_PYBSON_TO_CPPBSON( bson_hint, hint ) ;

   rc = cl->update( *rule, *condition, *hint ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( rule ) ;
   DELETE_CPPOBJECT( condition ) ;
   DELETE_CPPOBJECT( hint ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cl_upsert)
{
   INT32 rc                       = 0 ;
   PYOBJECT *obj                  = NULL ;
   PYOBJECT *bson_rule            = NULL ;
   PYOBJECT *bson_condition       = NULL ;
   PYOBJECT *bson_hint            = NULL ;
   sdbCollection *cl              = NULL ;
   const bson::BSONObj *rule      = NULL ;
   const bson::BSONObj *condition = NULL ;
   const bson::BSONObj *hint      = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OOOO", &obj, &bson_rule,
      &bson_condition, &bson_hint ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   CAST_PYBSON_TO_CPPBSON( bson_rule, rule ) ;
   CAST_PYBSON_TO_CPPBSON( bson_condition, condition ) ;
   CAST_PYBSON_TO_CPPBSON( bson_hint, hint ) ;

   rc = cl->upsert( *rule, *condition, *hint ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( rule ) ;
   DELETE_CPPOBJECT( condition ) ;
   DELETE_CPPOBJECT( hint ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cl_del)
{
   INT32 rc                       = 0 ;
   PYOBJECT *obj                  = NULL ;
   PYOBJECT *bson_condition       = NULL ;
   PYOBJECT *bson_hint            = NULL ;
   sdbCollection *cl              = NULL ;
   const bson::BSONObj *condition = NULL ;
   const bson::BSONObj *hint      = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OOO", &obj, &bson_condition, &bson_hint ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   CAST_PYBSON_TO_CPPBSON( bson_condition, condition ) ;
   CAST_PYBSON_TO_CPPBSON( bson_hint, hint ) ;

   rc = cl->del( *condition, *hint ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( condition ) ;
   DELETE_CPPOBJECT( hint ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cl_query)
{
   INT32 rc                       = 0 ;
   INT64 num_to_skip              = 0 ;
   INT64 num_to_return            = -1 ;
   PYOBJECT *obj                  = NULL ;
   PYOBJECT *cursor_object        = NULL ;
   PYOBJECT *bson_condition       = NULL ;
   PYOBJECT *bson_selector        = NULL ;
   PYOBJECT *bson_order_by        = NULL ;
   PYOBJECT *bson_hint            = NULL ;
   sdbCollection *cl              = NULL ;
   sdbCursor *cursor              = NULL ;
   const bson::BSONObj *condition = NULL ;
   const bson::BSONObj *selector  = NULL ;
   const bson::BSONObj *order_by  = NULL ;
   const bson::BSONObj *hint      = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OOOOOOLL", &obj, &cursor_object,
      &bson_condition,  &bson_selector, &bson_order_by,
      &bson_hint, &num_to_skip, &num_to_return ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   CAST_PYOBJECT_TO_COBJECT( cursor_object, sdbCursor, cursor ) ;
   CAST_PYBSON_TO_CPPBSON( bson_condition, condition ) ;
   CAST_PYBSON_TO_CPPBSON( bson_selector, selector ) ;
   CAST_PYBSON_TO_CPPBSON( bson_order_by, order_by ) ;
   CAST_PYBSON_TO_CPPBSON( bson_hint, hint ) ;

   rc = cl->query( *cursor, *condition, *selector, *order_by, *hint,
      num_to_skip, num_to_return ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( condition ) ;
   DELETE_CPPOBJECT( selector ) ;
   DELETE_CPPOBJECT( order_by ) ;
   DELETE_CPPOBJECT( hint ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cl_create_index)
{
   INT32 rc                       = 0 ;
   BOOLEAN is_unique              = 0 ;
   BOOLEAN is_enforced            = 0 ;
   PYOBJECT *obj                  = NULL ;
   PYOBJECT *bson_index_def       = NULL ;
   sdbCollection *cl              = NULL ;
   const bson::BSONObj *index_def = NULL ;
   const CHAR *name               = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OOsii", &obj, &bson_index_def, &name,
      &is_unique, &is_enforced ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   CAST_PYBSON_TO_CPPBSON( bson_index_def, index_def ) ;

   rc = cl->createIndex( *index_def, name, is_unique, is_enforced ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( index_def ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cl_get_index)
{
   INT32 rc                = 0 ;
   PYOBJECT *obj           = NULL ;
   PYOBJECT *cursor_object = NULL ;
   sdbCollection *cl       = NULL ;
   sdbCursor *cursor       = NULL ;
   const CHAR *index_name  = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OOs", &obj, &cursor_object, &index_name ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   CAST_PYOBJECT_TO_COBJECT( cursor_object, sdbCursor, cursor ) ;

   if ( 0 == ossStrncmp("", index_name, ossStrlen(index_name) ) )
   {
      index_name = NULL ;
   }

   rc = cl->getIndexes( *cursor, index_name ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cl_drop_index)
{
   INT32 rc               = 0 ;
   PYOBJECT *obj          = NULL ;
   sdbCollection *cl      = NULL ;
   const CHAR *index_name = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "Os", &obj, &index_name ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;

   rc = cl->dropIndex( index_name ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cl_get_collection_name)
{
   INT32 rc            = 0 ;
   PYOBJECT *obj       = NULL ;
   sdbCollection *cl   = NULL ;
   const CHAR *cl_name = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;

   cl_name = cl->getCollectionName() ;

done:
   return MAKE_RETURN_INT_PYSTRING( rc, cl_name ) ;
}

__METHOD_IMP(cl_get_collection_space_name)
{
   INT32 rc            = 0 ;
   PYOBJECT *obj       = NULL ;
   sdbCollection *cl   = NULL ;
   const CHAR *cs_name = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;

   cs_name = cl->getCSName() ;

done:
   return MAKE_RETURN_INT_PYSTRING( rc, cs_name ) ;
}

__METHOD_IMP(cl_get_full_name)
{
   INT32 rc              = 0 ;
   PYOBJECT *obj         = NULL ;
   sdbCollection *cl     = NULL ;
   const CHAR *full_name = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;

   full_name = cl->getFullName() ;

done:
   return MAKE_RETURN_INT_PYSTRING( rc, full_name ) ;
}

__METHOD_IMP(cl_aggregate)
{
   INT32 rc                = 0 ;
   PYOBJECT *obj           = NULL ;
   PYOBJECT *list_object   = NULL ;
   PYOBJECT *cursor_object = NULL ;
   sdbCollection *cl       = NULL ;
   sdbCursor *cursor       = NULL ;

   std::vector< bson::BSONObj > vec_bson ;

   if ( !PARSE_PYTHON_ARGS( args, "OOO", &obj, &cursor_object, &list_object ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   CAST_PYOBJECT_TO_COBJECT( cursor_object, sdbCursor, cursor ) ;
   MAKE_PYLIST_TO_VECTOR( list_object, vec_bson ) ;
   rc = cl->aggregate( *cursor, vec_bson ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cl_get_query_meta)
{
   INT32 rc                       = 0 ;
   INT64 num_to_skip              = 0 ;
   INT64 num_to_return            = -1 ;
   PYOBJECT *obj                  = NULL ;
   PYOBJECT *cursor_object        = NULL ;
   PYOBJECT *bson_condition       = NULL ;
   PYOBJECT *bson_order_by        = NULL ;
   PYOBJECT *bson_hint            = NULL ;
   sdbCollection *cl              = NULL ;
   sdbCursor *cursor              = NULL ;
   const bson::BSONObj *condition = NULL ;
   const bson::BSONObj *order_by  = NULL ;
   const bson::BSONObj *hint      = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OOOOOLL", &obj, &cursor_object,
      &bson_condition, &bson_order_by, &bson_hint,
      &num_to_skip, &num_to_return ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   CAST_PYOBJECT_TO_COBJECT( cursor_object, sdbCursor, cursor ) ;
   CAST_PYBSON_TO_CPPBSON( bson_condition, condition ) ;
   CAST_PYBSON_TO_CPPBSON( bson_order_by, order_by ) ;
   CAST_PYBSON_TO_CPPBSON( bson_hint, hint ) ;

   rc = cl->getQueryMeta( *cursor, *condition, *order_by, *hint,
      num_to_skip, num_to_return ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( condition ) ;
   DELETE_CPPOBJECT( order_by ) ;
   DELETE_CPPOBJECT( hint ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cl_attach_collection)
{
   INT32 rc                    = 0 ;
   PYOBJECT *obj               = NULL ;
   PYOBJECT *bson_option       = NULL ;
   const CHAR *sub_full_name   = NULL ;
   sdbCollection *cl           = NULL ;
   const bson::BSONObj *option = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OsO", &obj, &sub_full_name, &bson_option ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }
   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   CAST_PYBSON_TO_CPPBSON( bson_option, option ) ;

   rc = cl->attachCollection( sub_full_name, *option ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( option ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cl_detach_collection)
{
   INT32 rc                  = 0 ;
   PYOBJECT *obj             = NULL ;
   const CHAR *sub_full_name = NULL ;
   sdbCollection *cl         = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "Os", &obj, &sub_full_name ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }
   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;

   rc = cl->detachCollection( sub_full_name ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cl_create_lob)
{
   INT32 rc           = SDB_OK ;
   PYOBJECT *obj      = NULL ;
   PYOBJECT *obj_lob  = NULL ;
   PYOBJECT *oid_obj  = NULL ;
   sdbCollection *cl  = NULL ;
   sdbLob *lob        = NULL ;
   const CHAR * str_id= NULL ;
   bson::OID *pOid    = NULL ;
   bson::OID oid;
   

   if ( !PARSE_PYTHON_ARGS(args, "OOO", &obj, &obj_lob, &oid_obj) )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   CAST_PYOBJECT_TO_COBJECT( obj_lob, sdbLob, lob ) ;
   if ( Py_None != oid_obj )
   {
      str_id = PyString_AsString(oid_obj) ;
      if ( NULL != str_id )
      {
         oid.init( str_id ) ;
         pOid = &oid ;
      }
   }

   rc = cl->createLob(*lob, pOid) ;

done:
   return MAKE_RETURN_INT(rc) ;
error:
   goto done ;
}

__METHOD_IMP(cl_get_lob)
{
   INT32 rc           = SDB_OK ;
   PYOBJECT *obj      = NULL ;
   PYOBJECT *obj_lob  = NULL ;
   sdbCollection *cl  = NULL ;
   sdbLob *lob        = NULL ;
   const CHAR *str_id = NULL ;
   bson::OID oid;

   if ( !PARSE_PYTHON_ARGS(args, "OOs", &obj, &obj_lob, &str_id) )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   CAST_PYOBJECT_TO_COBJECT( obj_lob, sdbLob, lob ) ;
   oid.init(str_id) ;

   rc = cl->openLob(*lob, oid) ;

done:
   return MAKE_RETURN_INT(rc) ;
error:
   goto done ;
}

__METHOD_IMP(cl_remove_lob)
{
   INT32 rc           = SDB_OK ;
   PYOBJECT *obj      = NULL ;
   sdbCollection *cl  = NULL ;
   const CHAR *str_id = NULL ;
   bson::OID oid ;

   if ( !PARSE_PYTHON_ARGS(args, "Os", &obj, &str_id) )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   oid.init(str_id) ;
   rc = cl->removeLob( oid ) ;

done:
   return MAKE_RETURN_INT(rc) ;
error:
   goto done ;
}

__METHOD_IMP(cl_list_lobs)
{
   INT32 rc           = SDB_OK ;
   PYOBJECT *obj      = NULL ;
   PYOBJECT *obj_cr   = NULL ;
   sdbCollection *cl  = NULL ;
   sdbCursor  *cursor = NULL ;

   if ( !PARSE_PYTHON_ARGS(args, "OO", &obj, &obj_cr) )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   CAST_PYOBJECT_TO_COBJECT( obj_cr, sdbCursor, cursor) ;
   rc = cl->listLobs( *cursor ) ;

done:
   return MAKE_RETURN_INT(rc) ;
error:
   goto done ;
}

__METHOD_IMP(cl_explain)
{
   INT32 rc                       = 0 ;
   INT64 num_to_skip              = 0 ;
   INT64 num_to_return            = -1 ;
   INT32 flag                     = 0 ;
   PYOBJECT *obj                  = NULL ;
   PYOBJECT *cursor_object        = NULL ;
   PYOBJECT *bson_condition       = NULL ;
   PYOBJECT *bson_selector        = NULL ;
   PYOBJECT *bson_order_by        = NULL ;
   PYOBJECT *bson_hint            = NULL ;
   PYOBJECT *bson_options         = NULL ;
   sdbCollection *cl              = NULL ;
   sdbCursor *cursor              = NULL ;
   const bson::BSONObj *condition = NULL ;
   const bson::BSONObj *selector  = NULL ;
   const bson::BSONObj *order_by  = NULL ;
   const bson::BSONObj *hint      = NULL ;
   const bson::BSONObj *options   = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OOOOOOLLLO", &obj, &cursor_object,
        &bson_condition,  &bson_selector, &bson_order_by,
        &bson_hint, &num_to_skip, &num_to_return, &flag, &bson_options ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCollection, cl ) ;
   CAST_PYOBJECT_TO_COBJECT( cursor_object, sdbCursor, cursor ) ;
   CAST_PYBSON_TO_CPPBSON( bson_condition, condition ) ;
   CAST_PYBSON_TO_CPPBSON( bson_selector, selector ) ;
   CAST_PYBSON_TO_CPPBSON( bson_order_by, order_by ) ;
   CAST_PYBSON_TO_CPPBSON( bson_hint, hint ) ;
   CAST_PYBSON_TO_CPPBSON( bson_options, options ) ;

   rc = cl->explain( *cursor, *condition, *selector, *order_by, *hint,
                     num_to_skip, num_to_return, flag, *options ) ;
   if ( rc )
   {
      goto done ;
   }

done:
   DELETE_CPPOBJECT( condition ) ;
   DELETE_CPPOBJECT( selector ) ;
   DELETE_CPPOBJECT( order_by ) ;
   DELETE_CPPOBJECT( hint ) ;
   DELETE_CPPOBJECT( options ) ;
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(create_cursor)
{
   sdbCursor *cursor = NULL;
   if ( !PARSE_PYTHON_ARGS(args, "") )
   {
      return NULL ;
   }

   NEW_CPPOBJECT( cursor, sdbCursor ) ;
   if ( NULL == cursor )
   {
      return NULL ;
   }

   return MAKE_PYOBJECT( cursor ) ;
}

__METHOD_IMP(release_cursor)
{
   INT32 rc          = 0 ;
   PYOBJECT *obj     = NULL ;
   sdbCursor *cursor = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCursor, cursor ) ;
   DELETE_CPPOBJECT( cursor ) ;
done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(cr_next)
{
   INT32 rc          = 0 ;
   PYOBJECT *obj     = NULL ;
   sdbCursor *cursor = NULL ;
   bson::BSONObj retObj ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCursor, cursor ) ;
   rc = cursor->next( retObj ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

done :
   return MAKE_RETURN_INT_PYSTRING_SIZE( rc, retObj.objdata(), 
      retObj.objsize() ) ;
error :
   goto done ;
}

__METHOD_IMP(cr_current)
{
   INT32 rc          = 0 ;
   PYOBJECT *obj     = NULL ;
   sdbCursor *cursor = NULL ;
   bson::BSONObj bson ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCursor, cursor ) ;
   rc = cursor->current( bson ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   return MAKE_RETURN_INT_PYSTRING_SIZE( rc, bson.objdata(), bson.objsize() ) ;
error :
   goto done ;
}

__METHOD_IMP(cr_close)
{
   INT32 rc          = 0 ;
   PYOBJECT *obj     = NULL ;
   sdbCursor *cursor = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbCursor, cursor ) ;
   rc = cursor->close( ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   return MAKE_RETURN_INT( rc ) ;
error :
   goto done ;
}

typedef sdbReplicaGroup Group ;
__METHOD_IMP(create_group)
{
   Group *replica_group = NULL;
   if ( !PARSE_PYTHON_ARGS(args, "") )
   {
      return NULL ;
   }

   NEW_CPPOBJECT( replica_group, Group ) ;
   if ( NULL == replica_group )
   {
      return NULL ;
   }

   return MAKE_PYOBJECT( replica_group ) ;
}

__METHOD_IMP(release_group)
{
   INT32 rc             = 0 ;
   PYOBJECT *obj        = NULL ;
   Group *replica_group = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, Group, replica_group ) ;
   DELETE_CPPOBJECT( replica_group ) ;
done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(gp_get_nodenum)
{
   INT32 rc             = 0 ;
   PYOBJECT *obj        = NULL ;
   INT32 nodestatus     = SDB_NODE_UNKNOWN ;
   INT32 nodenum        = 0 ;
   Group *replica_group = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "Oi", &obj, &nodestatus ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, Group, replica_group ) ;
   rc = replica_group->getNodeNum( (sdbNodeStatus)nodestatus, &nodenum ) ;
done :
   return MAKE_RETURN_INT_INT(rc, nodenum) ;
error :
   goto done ;
}

__METHOD_IMP(gp_get_detail)
{
   INT32 rc             = 0 ;
   PYOBJECT *obj        = NULL ;
   bson::BSONObj bson ;
   Group *replica_group = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, Group, replica_group ) ;
   rc = replica_group->getDetail( bson ) ;
done :
   return MAKE_RETURN_INT_PYSTRING_SIZE( rc, bson.objdata(), 
      bson.objsize() ) ;
error :
   goto done ;
}

static INT32 convert_pobj2cobj( PYOBJECT *self, PYOBJECT *args, 
                                Group *& group, sdbNode *& node)
{
   INT32 rc            = 0 ;
   PYOBJECT *group_obj = NULL ;
   PYOBJECT *node_obj  = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OO", &group_obj, &node_obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( group_obj, Group, group ) ;
   CAST_PYOBJECT_TO_COBJECT( node_obj, sdbNode, node ) ;
done :
   return rc;
error :
   goto done ;
}

static INT32 pydict_to_cmap( PYOBJECT *pyobj,
                            std::map<std::string,std::string>& cobj )
{
   INT32 rc = SDB_OK ;
   PyObject *key, *keys;

   if ( !PyDict_Check( pyobj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }   

   keys = PyDict_Keys( pyobj );
   for ( int i = 0; i < PyList_GET_SIZE( keys ); ++i )
   {
      key = PyList_GET_ITEM( keys, i ) ;
      const CHAR *key_name = PyString_AsString( key );

      PyObject *val = PyDict_GetItemString( pyobj, key_name );
      if ( NULL == val || !PyString_Check( val ) )
      {
         rc = SDB_INVALIDARGS ;      
         goto error ;
      }
      cobj[ key_name ] = PyString_AsString( val );
   }
done :
   return rc ;
error :
   goto done ;
}

__METHOD_IMP(gp_get_master)
{
   INT32 rc             = 0 ;
   sdbNode *node        = NULL ;
   Group *replica_group = NULL ;

   rc = convert_pobj2cobj( self, args, replica_group, node) ;
   if  ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = replica_group->getMaster( *node ) ;
   if  ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   return MAKE_RETURN_INT( rc ) ;
error :
   goto done ;
}

__METHOD_IMP(gp_get_slave)
{
   INT32 rc             = 0 ;
   sdbNode *node        = NULL ;
   Group *replica_group = NULL ;

   rc = convert_pobj2cobj( self, args, replica_group, node) ;
   if  ( SDB_OK != rc )
   {
      goto error ;
   }
   rc = replica_group->getSlave( *node ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   return MAKE_RETURN_INT( rc ) ;
error :
   goto done ;
}

__METHOD_IMP(gp_get_node_by_name)
{
   INT32 rc             = 0 ;
   PYOBJECT *group_obj  = NULL ;
   PYOBJECT *node_obj   = NULL ;
   const CHAR *nodename = NULL ;
   sdbNode *node        = NULL ;
   Group *replica_group = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OOs", &group_obj, &node_obj, &nodename ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( group_obj, Group, replica_group ) ;
   CAST_PYOBJECT_TO_COBJECT( node_obj, sdbNode, node ) ;

   rc = replica_group->getNode( nodename, *node ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   return MAKE_RETURN_INT( rc ) ;
error :
   goto done ;
}

__METHOD_IMP(gp_get_node_by_endpoint)
{
   INT32 rc                = 0 ;
   PYOBJECT *group_obj     = NULL ;
   PYOBJECT *node_obj      = NULL ;
   const CHAR *hostname    = NULL ;
   const CHAR *servicename = NULL ;
   sdbNode *node           = NULL ;
   Group *replica_group    = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OOss", &group_obj, &node_obj, 
      &hostname, &servicename ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( group_obj, Group, replica_group ) ;
   CAST_PYOBJECT_TO_COBJECT( node_obj, sdbNode, node ) ;
   rc = replica_group->getNode( hostname, servicename, *node ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   return MAKE_RETURN_INT( rc ) ;
error :
   goto done ;
}

__METHOD_IMP(gp_create_node)
{
   INT32 rc                = 0 ;
   PYOBJECT *obj           = NULL ;
   const CHAR *nodename    = NULL ;
   const CHAR *servicename = NULL ;
   const CHAR *nodepath    = NULL ;
   PYOBJECT *dict          = NULL ;
   std::map<std::string,std::string> config ;
   Group *replica_group    = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OsssO", &obj, &nodename, 
      &servicename, &nodepath, &dict ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, Group, replica_group ) ;

   rc = pydict_to_cmap( dict, config ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }

   rc = replica_group->createNode( nodename, servicename, nodepath, config ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done :
   return MAKE_RETURN_INT( rc ) ;
error :
   goto done ;
}

__METHOD_IMP(gp_remove_node)
{
   INT32 rc                = 0 ;
   PYOBJECT *obj           = NULL ;
   PYOBJECT *pybson        = NULL ;
   const CHAR *hostname    = NULL ;
   const CHAR *servicename = NULL ;
   Group *replica_group    = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "Oss|O", &obj, &hostname, &servicename, pybson ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbReplicaGroup, replica_group ) ;
   if ( NULL == pybson )
   {
      rc = replica_group->removeNode( hostname, servicename ) ;
   }
   else
   {
      const CHAR *bson_string = PyBytes_AsString( pybson );
      bson::BSONObj cbson( bson_string );

      rc = replica_group->removeNode( hostname, servicename, cbson ) ;
   }
done :
   return MAKE_RETURN_INT( rc ) ;
error :
   goto done ;
}

__METHOD_IMP(gp_start)
{
   INT32 rc                       = 0 ;
   PYOBJECT *obj                  = NULL ;
   sdbReplicaGroup *replica_group = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbReplicaGroup, replica_group ) ;
   rc = replica_group->start() ;
done :
   return MAKE_RETURN_INT( rc ) ;
error :
   goto done ;
}

__METHOD_IMP(gp_stop)
{
   INT32 rc                       = 0 ;
   PYOBJECT *obj                  = NULL ;
   sdbReplicaGroup *replica_group = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, Group, replica_group ) ;
   rc = replica_group->stop() ;
done :
   return MAKE_RETURN_INT( rc ) ;
error :
   goto done ;
}

__METHOD_IMP(gp_is_catalog)
{
   INT32 rc                       = 0 ;
   PYOBJECT *obj                  = NULL ;
   sdbReplicaGroup *replica_group = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, Group, replica_group ) ;
   rc = replica_group->isCatalog() ;
done :
   return MAKE_RETURN_INT( rc ) ;
error :
   goto done ;
}

__METHOD_IMP(create_node)
{
   sdbNode *node = NULL;
   if ( !PARSE_PYTHON_ARGS(args, "") )
   {
      return NULL ;
   }

   NEW_CPPOBJECT( node, sdbNode ) ;
   if ( NULL == node )
   {
      return NULL ;
   }

   return MAKE_PYOBJECT( node ) ;
}

__METHOD_IMP(release_node)
{
   INT32 rc       = 0 ;
   PYOBJECT *obj  = NULL ;
   sdbNode  *node = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbNode, node ) ;
   DELETE_CPPOBJECT( node ) ;
done:
   return MAKE_RETURN_INT( rc ) ;
}

__METHOD_IMP(nd_connect)
{
   INT32 rc         = 0 ;
   PYOBJECT *obj    = NULL ;
   PYOBJECT *sdbodj = NULL ;
   sdbNode *node    = NULL ;
   sdb *client      = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "OO", &obj, &sdbodj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbNode, node ) ;
   CAST_PYOBJECT_TO_COBJECT( sdbodj, sdb, client ) ;
   rc = node->connect( *client ) ;
done :
   return MAKE_RETURN_INT( rc ) ;
error :
   goto done ;
}

__METHOD_IMP(nd_get_status)
{
   INT32  rc        = 0 ;
   PYOBJECT *obj    = NULL ;
   INT32 nodestatus = SDB_NODE_UNKNOWN ;
   sdbNode *node    = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbNode, node ) ;
   nodestatus = node->getStatus() ;
done :
   return MAKE_RETURN_INT_INT( rc, nodestatus ) ;
error :
   goto done ;
}

__METHOD_IMP(nd_get_hostname)
{
   INT32  rc            = 0 ;
   PYOBJECT *obj        = NULL ;
   const CHAR *hostname = "" ;
   sdbNode *node        = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbNode, node ) ;
   hostname = node->getHostName() ;
done :
   return MAKE_RETURN_INT_PYSTRING( rc, hostname ) ;
error :
   goto done ;
}

__METHOD_IMP(nd_get_servicename)
{
   INT32  rc               = 0 ;
   PYOBJECT *obj           = NULL ;
   const CHAR *servicename = "" ;
   sdbNode *node           = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbNode, node ) ;
   servicename = node->getServiceName() ;
done :
   return MAKE_RETURN_INT_PYSTRING( rc, servicename ) ;
error :
   goto done ;
}

__METHOD_IMP(nd_get_nodename)
{
   INT32  rc            = 0 ;
   PYOBJECT *obj        = NULL ;
   const CHAR *nodename = "" ;
   sdbNode *node        = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbNode, node ) ;
   nodename = node->getNodeName() ;
done :
   return MAKE_RETURN_INT_PYSTRING( rc, nodename ) ;
error :
   goto done ;
}

__METHOD_IMP(nd_stop)
{
   INT32 rc      = 0 ;
   PYOBJECT *obj = NULL ;
   sdbNode *node = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbNode, node ) ;
   rc = node->stop() ;
done :
   return MAKE_RETURN_INT( rc ) ;
error :
   goto done ;
}

__METHOD_IMP(nd_start)
{
   INT32 rc      = 0 ;
   PYOBJECT *obj = NULL ;
   sdbNode *node = NULL ;

   if ( !PARSE_PYTHON_ARGS( args, "O", &obj ) )
   {
      rc = SDB_INVALIDARGS ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT( obj, sdbNode, node ) ;
   rc = node->start() ;
done :
   return MAKE_RETURN_INT( rc ) ;
error :
   goto done ;
}


__METHOD_IMP(lob_create)
{
   sdbLob *ret_obj    = NULL ;
   NEW_CPPOBJECT(ret_obj, sdbLob);
   if ( NULL == ret_obj )
   {
      return NULL ;
   }

   return MAKE_PYOBJECT(ret_obj) ;
}

__METHOD_IMP(lob_release)
{
   INT32 rc           = SDB_OK ;
   PYOBJECT *obj      = NULL ;
   sdbLob *lob        = NULL ;

   if ( !PARSE_PYTHON_ARGS(args, "O", &obj))
   {
      rc = SDB_INVALIDARG ;
      goto done ;
   }

   CAST_PYOBJECT_TO_COBJECT(obj, sdbLob, lob) ;
   DELETE_CPPOBJECT(lob) ;
done:
   return MAKE_RETURN_INT(rc) ;
}

__METHOD_IMP(lob_close)
{
   INT32 rc = SDB_OK ;
   PYOBJECT *obj = NULL ;
   sdbLob *lob = NULL ;

   if ( !PARSE_PYTHON_ARGS(args, "O", &obj) )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT(obj, sdbLob, lob) ;
   rc = lob->close() ;

done:
   return MAKE_RETURN_INT( rc ) ;
error:
   goto done ;
}

__METHOD_IMP(lob_read)
{
   INT32 rc = SDB_OK ;
   PYOBJECT *obj = NULL ;
   sdbLob *lob = NULL ;
   UINT32 len = 0 ;
   UINT32 realLen = 0 ;
   CHAR *buffer = NULL ;
   std::string str_data;

   if ( !PARSE_PYTHON_ARGS(args, "OI", &obj, &len ) )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   if ( len <= 0 )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT(obj, sdbLob, lob) ;
   buffer = new CHAR[len + 1] ;
   ossMemset( buffer, 0, len + 1) ;
   rc = lob->read( len, buffer, &realLen ) ;
   str_data = buffer ;

done:
   if ( NULL != buffer )
   {
      delete [] buffer ;
      buffer = NULL ;
   }
   return MAKE_RETURN_INT_PYSTRING_UINT( rc, str_data.c_str(), realLen ) ;
error:
   goto done ;
}

__METHOD_IMP(lob_write)
{
   INT32 rc = SDB_OK ;
   PYOBJECT *obj = NULL ;
   sdbLob *lob = NULL ;
   INT32 len = 0 ;
   INT32 realLen = 0 ;
   CHAR *str = NULL ;

   if ( !PARSE_PYTHON_ARGS(args, "Osi", &obj, &str, &len ) )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   if ( len <= 0 )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT(obj, sdbLob, lob) ;
   rc = lob->write( str, len ) ;

done:
   return MAKE_RETURN_INT( rc ) ;
error:
   goto done ;
}

__METHOD_IMP(lob_seek)
{
   INT32 rc = SDB_OK ;
   PYOBJECT *obj = NULL ;
   sdbLob *lob   = NULL ;
   SINT64 offset = 0 ;
   INT32 whence = 0 ;

   if ( !PARSE_PYTHON_ARGS(args, "Oli", &obj, &offset, &whence))
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT(obj, sdbLob, lob) ;
   rc = lob->seek(offset, (SDB_LOB_SEEK)whence) ;
   
done:
   return MAKE_RETURN_INT( rc ) ;
error:
   goto done ;
}

__METHOD_IMP(lob_get_create_time)
{
   INT32 rc = SDB_OK ;
   UINT64 ms = 0;
   PYOBJECT *obj = NULL ;
   sdbLob  *lob  = NULL ;

   if ( !PARSE_PYTHON_ARGS(args, "O", &obj) )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT(obj, sdbLob, lob) ;
   rc = lob->getCreateTime(&ms) ;
   
done:
   return MAKE_RETURN_INT_ULLONG( rc, ms ) ;
error:
   goto done ;
}

__METHOD_IMP(lob_get_size)
{
   INT32 rc = SDB_OK ;
   SINT64 lobSize = 0;
   PYOBJECT *obj = NULL ;
   sdbLob  *lob  = NULL ;

   if ( !PARSE_PYTHON_ARGS(args, "O", &obj) )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT(obj, sdbLob, lob) ;
   rc = lob->getSize(&lobSize) ;

done:
   return MAKE_RETURN_INT_LONG( rc, lobSize ) ;
error:
   goto done ;
}

__METHOD_IMP(lob_get_oid)
{
   INT32 rc = SDB_OK ;
   UINT64 ms = 0;
   PYOBJECT *obj = NULL ;
   sdbLob  *lob  = NULL ;
   bson::OID oid ;

   if ( !PARSE_PYTHON_ARGS(args, "O", &obj) )
   {
      rc = SDB_INVALIDARG ;
      goto error ;
   }

   CAST_PYOBJECT_TO_COBJECT(obj, sdbLob, lob) ;
   rc = lob->getOid(oid) ;

done:
   return MAKE_RETURN_INT_PYSTRING( rc, oid.str().c_str()) ;
error:
   goto done ;
}

static PyMethodDef sqldb_methods[] = {
   /** client */
   {"sdb_create_client",               sdb_create_client,               METH_VARARGS},
   {"sdb_release_client",              sdb_release_client,              METH_VARARGS},
   {"sdb_connect",                     sdb_connect,                     METH_VARARGS},
   {"sdb_disconnect",                  sdb_disconnect,                  METH_VARARGS},
   {"sdb_create_user",                 sdb_create_user,                 METH_VARARGS},
   {"sdb_remove_user",                 sdb_remove_user,                 METH_VARARGS},
   {"sdb_get_snapshot",                sdb_get_snapshot,                METH_VARARGS},
   {"sdb_reset_snapshot",              sdb_reset_snapshot,              METH_VARARGS},
   {"sdb_get_list",                    sdb_get_list,                    METH_VARARGS},
   {"sdb_get_collection_space",        sdb_get_collection_space,        METH_VARARGS},
   {"sdb_get_collection",              sdb_get_collection,              METH_VARARGS},
   {"sdb_create_collection_space",     sdb_create_collection_space,     METH_VARARGS},
   {"sdb_drop_collection_space",       sdb_drop_collection_space,       METH_VARARGS},
   {"sdb_list_collection_spaces",      sdb_list_collection_spaces,      METH_VARARGS},
   {"sdb_list_collections",            sdb_list_collections,            METH_VARARGS},
   {"sdb_list_replica_groups",         sdb_list_replica_groups,         METH_VARARGS},
   {"sdb_get_replica_group_by_name",   sdb_get_replica_group_by_name,   METH_VARARGS},
   {"sdb_get_replica_group_by_id",     sdb_get_replica_group_by_id,     METH_VARARGS},
   {"sdb_create_replica_group",        sdb_create_replica_group,        METH_VARARGS},
   {"sdb_remove_replica_group",        sdb_remove_replica_group,        METH_VARARGS},
   {"sdb_create_replica_cata_group",   sdb_create_replica_cata_group,   METH_VARARGS},
   {"sdb_exec_update",                 sdb_exec_update,                 METH_VARARGS},
   {"sdb_exec_sql",                    sdb_exec_sql,                    METH_VARARGS},
   {"sdb_transaction_begin",           sdb_transaction_begin,           METH_VARARGS},
   {"sdb_transaction_commit",          sdb_transaction_commit,          METH_VARARGS},
   {"sdb_transaction_rollback",        sdb_transaction_rollback,        METH_VARARGS},
   {"sdb_flush_configure",             sdb_flush_configure,             METH_VARARGS},
   {"sdb_create_JS_procedure",         sdb_create_JS_procedure,         METH_VARARGS},
   {"sdb_remove_procedure",            sdb_remove_procedure,            METH_VARARGS},
   {"sdb_list_procedures",             sdb_list_procedures,             METH_VARARGS},
   {"sdb_eval_JS",                     sdb_eval_JS,                     METH_VARARGS},
   {"sdb_backup_offline",              sdb_backup_offline,              METH_VARARGS},
   {"sdb_list_backup",                 sdb_list_backup,                 METH_VARARGS},
   {"sdb_remove_backup",               sdb_remove_backup,               METH_VARARGS},
   {"sdb_list_tasks",                  sdb_list_tasks,                  METH_VARARGS},
   {"sdb_wait_task",                   sdb_wait_task,                   METH_VARARGS},
   {"sdb_cancel_task",                 sdb_cancel_task,                 METH_VARARGS},
   {"sdb_set_session_attri",           sdb_set_session_attri,           METH_VARARGS},
   {"sdb_close_all_cursors",           sdb_close_all_cursors,           METH_VARARGS},
   {"sdb_is_valid",                    sdb_is_valid,                    METH_VARARGS},
   {"sdb_get_version",                 sdb_get_version,                 METH_VARARGS},
   /** cs */
   {"create_cs",                       create_cs,                       METH_VARARGS},
   {"release_cs",                      release_cs,                      METH_VARARGS},
   {"cs_get_collection",               cs_get_collection,               METH_VARARGS},
   {"cs_create_collection",            cs_create_collection,            METH_VARARGS},
   {"cs_create_collection_use_opt",    cs_create_collection_use_opt,    METH_VARARGS},
   {"cs_drop_collection",              cs_drop_collection,              METH_VARARGS},
   {"cs_get_collection_space_name",    cs_get_collection_space_name,    METH_VARARGS},
   /** cl */
   {"create_cl",                       create_cl,                       METH_VARARGS},
   {"release_cl",                      release_cl,                      METH_VARARGS},
   {"cl_get_count",                    cl_get_count,                    METH_VARARGS},
   {"cl_split_by_condition",           cl_split_by_condition,           METH_VARARGS},
   {"cl_split_by_percent",             cl_split_by_percent,             METH_VARARGS},
   {"cl_split_async_by_condition",     cl_split_async_by_condition,     METH_VARARGS},
   {"cl_splite_async_by_percent",      cl_splite_async_by_percent,      METH_VARARGS},
   {"cl_bulk_insert",                  cl_bulk_insert,                  METH_VARARGS},
   {"cl_insert",                       cl_insert,                       METH_VARARGS},
   {"cl_update",                       cl_update,                       METH_VARARGS},
   {"cl_upsert",                       cl_upsert,                       METH_VARARGS},
   {"cl_delete",                       cl_del,                          METH_VARARGS},
   {"cl_query",                        cl_query,                        METH_VARARGS},
   {"cl_create_index",                 cl_create_index,                 METH_VARARGS},
   {"cl_get_index",                    cl_get_index,                    METH_VARARGS},
   {"cl_drop_index",                   cl_drop_index,                   METH_VARARGS},
   {"cl_get_collection_name",          cl_get_collection_name,          METH_VARARGS},
   {"cl_get_collection_space_name",    cl_get_collection_space_name,    METH_VARARGS},
   {"cl_get_full_name",                cl_get_full_name,                METH_VARARGS},
   {"cl_aggregate",                    cl_aggregate,                    METH_VARARGS},
   {"cl_get_query_meta",               cl_get_query_meta,               METH_VARARGS},
   {"cl_attach_collection",            cl_attach_collection,            METH_VARARGS},
   {"cl_detach_collection",            cl_detach_collection,            METH_VARARGS},
   {"cl_create_lob",                   cl_create_lob,                   METH_VARARGS},
   {"cl_get_lob",                      cl_get_lob,                      METH_VARARGS},
   {"cl_remove_lob",                   cl_remove_lob,                   METH_VARARGS},
   {"cl_list_lobs",                    cl_list_lobs,                    METH_VARARGS},
   {"cl_explain",                      cl_explain,                      METH_VARARGS},
   /** cr */
   {"create_cursor",                   create_cursor,                   METH_VARARGS},
   {"release_cursor",                  release_cursor,                  METH_VARARGS},
   {"cr_next",                         cr_next,                         METH_VARARGS},
   {"cr_current",                      cr_current,                      METH_VARARGS},
   {"cr_close",                        cr_close,                        METH_VARARGS},
   /** gp */
   {"create_group",                    create_group,                    METH_VARARGS},
   {"release_group",                   release_group,                   METH_VARARGS},
   {"gp_get_nodenum",                  gp_get_nodenum,                  METH_VARARGS},
   {"gp_get_detail",                   gp_get_detail,                   METH_VARARGS},
   {"gp_get_master",                   gp_get_master,                   METH_VARARGS},
   {"gp_get_slave",                    gp_get_slave,                    METH_VARARGS},
   {"gp_get_nodebyname",               gp_get_node_by_name,             METH_VARARGS},
   {"gp_get_nodebyendpoint",           gp_get_node_by_endpoint,         METH_VARARGS},
   {"gp_create_node",                  gp_create_node,                  METH_VARARGS},
   {"gp_remove_node",                  gp_remove_node,                  METH_VARARGS},
   {"gp_start",                        gp_start,                        METH_VARARGS},
   {"gp_stop",                         gp_stop,                         METH_VARARGS},
   {"gp_is_catalog",                   gp_is_catalog,                   METH_VARARGS},
   /** nd */
   {"create_node",                     create_node,                     METH_VARARGS},
   {"release_node",                    release_node,                    METH_VARARGS},
   {"nd_connect",                      nd_connect,                      METH_VARARGS},
   {"nd_get_status",                   nd_get_status,                   METH_VARARGS},
   {"nd_get_hostname",                 nd_get_hostname,                 METH_VARARGS},
   {"nd_get_servicename",              nd_get_servicename,              METH_VARARGS},
   {"nd_get_nodename",                 nd_get_nodename,                 METH_VARARGS},
   {"nd_stop",                         nd_stop,                         METH_VARARGS},
   {"nd_start",                        nd_start,                        METH_VARARGS},
   /** lob */
   {"create_lob",                      lob_create,                      METH_VARARGS},
   {"release_lob",                     lob_release,                     METH_VARARGS},
   {"lob_close",                       lob_close,                       METH_VARARGS},
   {"lob_read",                        lob_read,                        METH_VARARGS},
   {"lob_write",                       lob_write,                       METH_VARARGS},
   {"lob_seek",                        lob_seek,                        METH_VARARGS},
   {"lob_get_size",                    lob_get_size,                    METH_VARARGS},
   {"lob_get_oid",                     lob_get_oid,                     METH_VARARGS},
   {"lob_get_create_time",             lob_get_create_time,             METH_VARARGS},
   {NULL, NULL}
};

CREATE_MODULE( sdb, sqldb_methods )
