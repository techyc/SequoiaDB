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

   Source File Name = fmpJSVM.cpp

   Descriptive Name =

   When/how to use:

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          06/19/2013  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#include "fmpJSVM.hpp"
#include "spt.hpp"
#include "ossMem.hpp"
#include "sptConvertorHelper.hpp"
#include "pd.hpp"

using namespace bson ;

BSONObj GLOBAL_SDB ;

_fmpJSVM::_fmpJSVM()
:_engine( NULL ),
 _scope( NULL ),
 _cursor(NULL)
{
   _engine = engine::ScriptEngine::globalScriptEngine();
   _scope = _engine->newScope() ;
   if ( NULL == _scope )
   {
      PD_LOG( PDERROR, "failed to new scope" ) ;
      return ;
   }

   BSONObjBuilder builder ;
   builder.appendCode( FMP_FUNC_VALUE, "var db=new Sdb();" ) ;
   builder.append( FMP_FUNC_TYPE, FMP_FUNC_TYPE_JS ) ;
   GLOBAL_SDB = builder.obj() ;
   _setOK( TRUE ) ;
}

_fmpJSVM::~_fmpJSVM()
{
   SAFE_OSS_DELETE( _scope ) ;
   _cursor = NULL ;
}

INT32 _fmpJSVM::initGlobalDB( BSONObj &res )
{
   INT32 rc = SDB_OK ;
   rc = eval( GLOBAL_SDB, res ) ;
   if ( SDB_OK != rc )
   {
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

INT32 _fmpJSVM::eval( const BSONObj &func,
                      BSONObj &res )
{
   INT32 rc = SDB_OK ;
   BSONElement ele = func.getField( FMP_FUNC_VALUE ) ;
   BSONElement type ;
   jsval val ;
   CHAR *errMsg = NULL ;
   if ( ele.eoo() || Code != ele.type() )
   {
      PD_LOG( PDERROR, "invalid func type: d", ele.type() ) ;
      rc = SDB_INVALIDARG ;
      res = BSON( FMP_ERR_MSG << "type of element must be Code" <<
                  FMP_RES_CODE << rc ) ;
      goto error ;
   }

   type = func.getField( FMP_FUNC_TYPE ) ;
   if ( type.eoo() || NumberInt != type.type() ||
        FMP_FUNC_TYPE_JS != type.Int() )
   {
      rc = SDB_INVALIDARG ;
      res = BSON( FMP_ERR_MSG << "type of func must be JS" <<
                  FMP_RES_CODE << rc ) ;
      goto error ;
   }

   rc = _transCode2Str( ele, _cmd ) ;
   if ( SDB_OK != rc )
   {
      PD_LOG( PDERROR, "failed to trans code to str:%d", rc ) ;
      res = BSON( FMP_ERR_MSG << "trans code to str failed" <<
                  FMP_RES_CODE << rc ) ;
      goto error ;
   }

   rc = _scope->evaluate2( _cmd.c_str(), _cmd.length(),
                           1, &val, &errMsg ) ;
   if ( SDB_OK != rc )
   {
      const CHAR *pLastErr = _scope->getLastErrMsg() ;
      INT32 lastErrno = _scope->getLastError() ;

      if ( !*pLastErr && errMsg )
      {
         pLastErr = errMsg ;
      }
      res = BSON( FMP_ERR_MSG << pLastErr <<
                  FMP_RES_CODE << lastErrno ) ;
      goto error ;
   }

   if ( JSVAL_IS_NULL( val ) )
   {
      res = BSON( FMP_RES_TYPE << FMP_RES_TYPE_VOID ) ;
   }
   else if ( JSVAL_IS_VOID( val ) )
   {
      res = BSON( FMP_RES_TYPE << FMP_RES_TYPE_VOID ) ;
   }
   else if ( JSVAL_IS_INT( val ) )
   {
      INT32 i = 0 ;
      if ( !JS_ValueToInt32( _scope->context(), val, &i ) )
      {
         rc = SDB_SYS ;
         res = BSON( FMP_ERR_MSG << "failed to convert jsval to int32" <<
                     FMP_RES_CODE << rc ) ;
         goto error ;
      }
      res = BSON( FMP_RES_TYPE << FMP_RES_TYPE_NUMBER <<
                  FMP_RES_VALUE << i ) ;
   }
   else if ( JSVAL_IS_DOUBLE(val) )
   {
      jsdouble jsd = 0 ;
      FLOAT64 f = 0 ;
      if ( !JS_ValueToNumber( _scope->context(), val, &jsd ))
      {
         rc = SDB_SYS ;
         res = BSON( FMP_ERR_MSG << "failed to convert jsval to float" <<
                     FMP_RES_CODE << rc ) ;
         goto error ;
      }
      f = jsd ;
      res = BSON( FMP_RES_TYPE << FMP_RES_TYPE_NUMBER <<
                  FMP_RES_VALUE << f ) ;
   }
   else if ( JSVAL_IS_STRING(val))
   {
      std::string s ;
      if ( SDB_OK != JSVal2String( _scope->context(), val, s ) )
      {
         rc = SDB_SYS ;
         res = BSON( FMP_ERR_MSG << "failed to convert jsval to string" <<
                     FMP_RES_CODE << rc ) ;
         goto error ;
      }
      res = BSON( FMP_RES_TYPE << FMP_RES_TYPE_STR <<
                  FMP_RES_VALUE << s ) ;
   }
   else if ( JSVAL_IS_BOOLEAN(val) )
   {
      JSBool bp ;
      BSONObjBuilder builder ;
      if ( !JS_ValueToBoolean( _scope->context(), val, &bp ) )
      {
         rc = SDB_SYS ;
         res = BSON( FMP_ERR_MSG << "failed to convert jsval to boolean" <<
                     FMP_RES_CODE << rc ) ;
         goto error ;
      }

      builder.append( FMP_RES_TYPE, FMP_RES_TYPE_BOOL ) ;
      builder.appendBool( FMP_RES_VALUE, bp ) ;
      res = builder.obj() ;
   }
   else if ( JSVAL_IS_OBJECT(val) )
   {
      JSObject *obj ;
      if ( !JS_ValueToObject( _scope->context(), val, &obj ) )
      {
         rc = SDB_SYS ;
         res = BSON( FMP_ERR_MSG << "failed to convert jsval to object" <<
                     FMP_RES_CODE << rc ) ;
         goto error ;
      }

      if ( JSObjIsSdbObj( _scope->context(), obj ) )
      {
         if ( JSObjIsCursor( _scope->context(), obj ) )
         {
            rc = JSObj2Cursor( _scope->context(), obj, &_cursor ) ;
            if ( SDB_OK != rc )
            {
               res = BSON( FMP_ERR_MSG << "failed to convert jsobj to cursor"
                           << FMP_RES_CODE << rc ) ;
                goto error ;
            }
            res = BSON( FMP_RES_TYPE << FMP_RES_TYPE_RECORDSET ) ;
         }
         else if ( JSObjIsQuery( _scope->context(), obj ) )
         {
            jsval val ;
            JSObject *execRes = NULL ;
            if ( !JS_CallFunctionName( _scope->context(), obj,
                                        "_exec", 0, NULL, &val ) )
            {
               rc = SDB_SYS ;
               res = BSON( FMP_ERR_MSG << "failed to call function _exec" <<
                           FMP_RES_CODE << rc ) ;
               goto error ;
            }

            if ( !JSVAL_IS_OBJECT( val) )
            {
               rc = SDB_SYS ;
               res = BSON( FMP_ERR_MSG << "find()._exec() did not return a obj"
                           << FMP_RES_CODE << rc ) ;
               goto error ;
            }
            if ( !JS_ValueToObject( _scope->context(), val, &execRes ) )
            {
               rc = SDB_SYS ;
               res = BSON( FMP_ERR_MSG << "failed to convert exec jsval to object" <<
                           FMP_RES_CODE << rc ) ;
               goto error ;
            }
            rc = JSObj2Cursor( _scope->context(), execRes, &_cursor ) ;
            if ( SDB_OK != rc )
            {
               rc = SDB_SYS ;
               res = BSON( FMP_ERR_MSG << "failed to exec obj to object" <<
                           FMP_RES_CODE << rc ) ;
               goto error ;
            }
            res = BSON( FMP_RES_TYPE << FMP_RES_TYPE_RECORDSET ) ;
         }
         else if ( JSObjIsCS( _scope->context(), obj ) )
         {
            CHAR *cs = NULL ;
            rc = getCSNameFromObj( _scope->context(), obj, &cs ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to get csname from csobj" ) ;
               res = BSON( FMP_ERR_MSG <<
                           "failed to get csname from csobj" <<
                           FMP_RES_CODE << rc ) ;
               goto error ;
            } 
            res = BSON( FMP_RES_TYPE << FMP_RES_TYPE_CS <<
                        FMP_RES_VALUE << cs ) ;
            JS_free( _scope->context(), cs ) ;
         }
         else if ( JSObjIsCL( _scope->context(), obj ) )
         {
            CHAR *cl = NULL ;
            rc = getCLNameFromObj( _scope->context(), obj, &cl ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to get clname from clobj" ) ;
               res = BSON( FMP_ERR_MSG <<
                           "failed to get clname from clobj" <<
                           FMP_RES_CODE << rc ) ;
               goto error ;
            }
            res = BSON( FMP_RES_TYPE << FMP_RES_TYPE_CL <<
                        FMP_RES_VALUE << cl ) ;
            SDB_OSS_FREE( cl ) ;
         }
         else if ( JSObjIsRG( _scope->context(), obj ) )
         {
            CHAR *rg = NULL ;
            rc = getRGNameFromObj( _scope->context(), obj, &rg ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to get rgname from rgobj" ) ;
               res = BSON( FMP_ERR_MSG <<
                           "failed to get rgname from rgobj" <<
                           FMP_RES_CODE << rc ) ;
               goto error ;
            }
            res = BSON( FMP_RES_TYPE << FMP_RES_TYPE_RG <<
                        FMP_RES_VALUE << rg ) ;
            JS_free( _scope->context(), rg ) ;
         }
         else if ( JSObjIsRN( _scope->context(), obj ) )
         {
            CHAR *rn = NULL ;
            rc = getRNNameFromObj( _scope->context(), obj, &rn ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG( PDERROR, "failed to get rnname from rnobj" ) ;
               res = BSON( FMP_ERR_MSG <<
                           "failed to get rnname from rnobj" <<
                           FMP_RES_CODE << rc ) ;
               goto error ;
            }
            res = BSON( FMP_RES_TYPE << FMP_RES_TYPE_RN <<
                        FMP_RES_VALUE << rn ) ;
            SDB_OSS_FREE( rn ) ;
         }
         else
         {
            rc = SDB_INVALIDARG ;
            res = BSON( FMP_ERR_MSG << "this sdb obj can not used to be return"
                        << FMP_RES_CODE << rc ) ;
            goto error ;
         }
      }
      else if ( JSObjIsBsonobj( _scope->context(), obj ) )
      {
         CHAR *raw = NULL ;
         rc = getBsonRawFromBsonClass( _scope->context(), obj, &raw ) ;
         if ( SDB_OK != rc || NULL == raw )
         {
            PD_LOG( PDERROR, "failed to get bson raw from bson class" ) ;
            res = BSON( FMP_ERR_MSG <<
                    "failed to get bson raw from bson class" <<
                    FMP_RES_CODE << rc ) ;
            goto error ;
         }
         else
         {
            try
            {
               BSONObj o(raw) ;
               res = BSON( FMP_RES_TYPE << FMP_RES_TYPE_OBJ <<
                           FMP_RES_VALUE << o ) ;
            }
            catch ( std::exception &e )
            {
               rc = SDB_SYS ;
               PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
               res = BSON( FMP_ERR_MSG << e.what() << FMP_RES_CODE << rc ) ;
               goto error ;
            }
         }
      }
      else
      {
         CHAR *raw = NULL ;
         rc = JSObj2BsonRaw( _scope->context(), obj, &raw ) ;
         if ( SDB_OK != rc )
         {
            rc = SDB_SYS ;
            res = BSON( FMP_ERR_MSG << "failed to convert jsobj to bson" <<
                        FMP_RES_CODE << rc ) ;
            goto error ;
         }
         else
         {
            try
            {
               BSONObj o( raw ) ;
               res = BSON( FMP_RES_TYPE << FMP_RES_TYPE_OBJ <<
                           FMP_RES_VALUE << o ) ;
               free( raw ) ;
            }
            catch ( std::exception &e )
            {
               rc = SDB_SYS ;
               PD_LOG( PDERROR, "unexpected err happened:%s", e.what() ) ;
               res = BSON( FMP_ERR_MSG << e.what() << FMP_RES_CODE << rc ) ;
               goto error ;
            }
         }
      }
   }
   else
   {
      rc = SDB_INVALIDARG ;
      res = BSON( FMP_ERR_MSG << "unknown result type" <<
                  FMP_RES_CODE << rc ) ;
      goto error ;
   }

done:
   if ( NULL != errMsg )
   {
      free( errMsg ) ;
   }
   return rc ;
error:
   goto done ;
}

INT32 _fmpJSVM::fetch( BSONObj &res )
{
   INT32 rc = SDB_OK ;
   CHAR *raw = NULL ;
   rc = cursorNextRaw( _cursor, &raw ) ;
   if ( SDB_DMS_EOC == rc )
   {
      res = BSON( FMP_RES_CODE << rc ) ;
      goto error ;
   }
   else if ( SDB_OK == rc )
   {
      BSONObj o( raw ) ;
      res = BSON( FMP_RES_TYPE << FMP_RES_TYPE_OBJ <<
                  FMP_RES_VALUE << o ) ;
   }
   else
   {
      res = BSON( FMP_ERR_MSG << "failed to getnext" <<
                  FMP_RES_CODE << rc ) ;
      goto error ;
   }
done:
   if ( NULL != raw )
   {
      free( raw ) ;
   }
   return rc ;
error:
   goto done ;
}

INT32 _fmpJSVM::_transCode2Str( const BSONElement &ele,
                                std::string &str )
{
   INT32 rc = SDB_OK ;
   str = ele.toString(FALSE, TRUE ) ;
   return rc ;
}
