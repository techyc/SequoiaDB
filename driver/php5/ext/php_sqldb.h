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

/** \file php_sqldb.h
    \brief XXX
 */
#ifndef PHP_SEQUOIADB_H
#define PHP_SEQUOIADB_H

#define PHP_SEQUOIADB_VERSION "1.0.0"
extern zend_module_entry sqldb_module_entry;
#define phpext_sqldb_ptr &sqldb_module_entry

#ifdef PHP_WIN32
#define PHP_SEQUOIADB_API __declspec(dllexport)
#elif defined(__GNUC__) && __GNUC__ >= 4
#define PHP_SEQUOIADB_API __attribute__ ((visibility("default")))
#else
#define PHP_SEQUOIADB_API PHPAPI
#endif

#ifdef ZTS
#include "TSRM.h"
#endif

#define PHP_JSON_OUTPUT_ARRAY	 0
#define PHP_JSON_OUTPUT_OBJECT 1

#define PHP_GET_VALUE_ERROR -2
#define PHP_GET_VALUE_NOTFIND -1

#define RETURN_ARRAY_TYPE TRUE
#define RETURN_STRING_TYPE FALSE


PHP_SEQUOIADB_API void json_encode_array ( CHAR **buf,
                                           INT32 &bufSize,
                                           INT32 &leftLen,
                                           zval **val TSRMLS_DC ) ;
PHP_SEQUOIADB_API void php_json_encode ( CHAR **buf,
                                         INT32 &bufSize,
                                         INT32 &leftLen,
                                         zval *val TSRMLS_DC ) ;
PHP_SEQUOIADB_API BOOLEAN php_json_decode ( const CHAR *buf,
                                            zval **val TSRMLS_DC ) ;
PHP_SEQUOIADB_API BOOLEAN php_toJson ( CHAR **buf, zval *val TSRMLS_DC );
PHP_SEQUOIADB_API BOOLEAN php_jsonArr2Vector ( std::vector<char *> *pVector, zval *val TSRMLS_DC ) ;
PHP_SEQUOIADB_API INT32 key_get_value ( zval *val, const CHAR *key TSRMLS_DC ) ;
PHP_SEQUOIADB_API BOOLEAN php_toNum64 ( INT64 &num64, zval *val TSRMLS_DC ) ;
PHP_SEQUOIADB_API void map_encode_array(std::map<std::string,std::string> *pMap,
                                        zval **val TSRMLS_DC ) ;
PHP_SEQUOIADB_API void php_map_encode(std::map<std::string,std::string> *pMap,
                                      zval *val TSRMLS_DC ) ;
PHP_SEQUOIADB_API BOOLEAN php_to_map ( zval *val,
                                       std::map<std::string,std::string> *pMap TSRMLS_DC ) ;

PHP_MINIT_FUNCTION(sqldb);
PHP_MSHUTDOWN_FUNCTION(sqldb);
PHP_RINIT_FUNCTION(sqldb);
PHP_RSHUTDOWN_FUNCTION(sqldb);
PHP_MINFO_FUNCTION(sqldb);


PHP_METHOD ( SqlDB, __construct ) ;
PHP_METHOD ( SqlDB, __destruct ) ;
PHP_METHOD ( SqlDB, install ) ;
PHP_METHOD ( SqlDB, getError ) ;
PHP_METHOD ( SqlDB, connect ) ;
PHP_METHOD ( SqlDB, close ) ;
PHP_METHOD ( SqlDB, execSQL ) ;
PHP_METHOD ( SqlDB, execUpdateSQL ) ;
PHP_METHOD ( SqlDB, getSnapshot ) ;
PHP_METHOD ( SqlDB, getList ) ;
PHP_METHOD ( SqlDB, selectGroup ) ;
PHP_METHOD ( SqlDB, resetSnapshot ) ;
PHP_METHOD ( SqlDB, selectCS ) ;
PHP_METHOD ( SqlDB, listCSs ) ;
PHP_METHOD ( SqlDB, listCollections ) ;
PHP_METHOD ( SqlDB, createCataGroup ) ;
PHP_METHOD ( SqlDB, dropCollectionSpace ) ;
PHP_METHOD ( SqlDB, createDomain ) ;
PHP_METHOD ( SqlDB, dropDomain ) ;
PHP_METHOD ( SqlDB, getDomain ) ;
PHP_METHOD ( SqlDB, listDomains ) ;

PHP_METHOD ( SequoiaCS, __construct ) ;
PHP_METHOD ( SequoiaCS, __destruct ) ;
PHP_METHOD ( SequoiaCS, selectCollection ) ;
PHP_METHOD ( SequoiaCS, drop ) ;
PHP_METHOD ( SequoiaCS, dropCollection ) ;
PHP_METHOD ( SequoiaCS, getName ) ;

PHP_METHOD ( SequoiaCL, __construct ) ;
PHP_METHOD ( SequoiaCL, __destruct ) ;
PHP_METHOD ( SequoiaCL, insert ) ;
PHP_METHOD ( SequoiaCL, update ) ;
PHP_METHOD ( SequoiaCL, remove ) ;
PHP_METHOD ( SequoiaCL, find ) ;
PHP_METHOD ( SequoiaCL, split ) ;
PHP_METHOD ( SequoiaCL, drop ) ;
PHP_METHOD ( SequoiaCL, aggregate ) ;
PHP_METHOD ( SequoiaCL, createIndex ) ;
PHP_METHOD ( SequoiaCL, deleteIndex ) ;
PHP_METHOD ( SequoiaCL, getIndex ) ;
PHP_METHOD ( SequoiaCL, getCSName ) ;
PHP_METHOD ( SequoiaCL, getCollectionName ) ;
PHP_METHOD ( SequoiaCL, getFullName ) ;
PHP_METHOD ( SequoiaCL, count ) ;

PHP_METHOD ( SequoiaCursor, __construct ) ;
PHP_METHOD ( SequoiaCursor, __destruct ) ;
PHP_METHOD ( SequoiaCursor, getNext ) ;
PHP_METHOD ( SequoiaCursor, current ) ;

PHP_METHOD ( SequoiaDomain, alterDomain ) ;
PHP_METHOD ( SequoiaDomain, listCSInDomain ) ;
PHP_METHOD ( SequoiaDomain, listCLInDomain ) ;

PHP_METHOD ( SequoiaID, __construct ) ;
PHP_METHOD ( SequoiaID, __toString ) ;

PHP_METHOD ( SequoiaDate, __construct ) ;
PHP_METHOD ( SequoiaDate, __toString ) ;

PHP_METHOD ( SequoiaTimestamp, __construct ) ;
PHP_METHOD ( SequoiaTimestamp, __toString ) ;

PHP_METHOD ( SequoiaRegex, __construct ) ;
PHP_METHOD ( SequoiaRegex, __toString ) ;

PHP_METHOD ( SequoiaINT64, __construct ) ;
PHP_METHOD ( SequoiaINT64, __toString ) ;

/* ********* group ************ */

PHP_METHOD ( sequoiaGroup, getNodeNum ) ;
PHP_METHOD ( sequoiaGroup, getDetail ) ;
PHP_METHOD ( sequoiaGroup, getMaster ) ;
PHP_METHOD ( sequoiaGroup, getSlave ) ;
PHP_METHOD ( sequoiaGroup, getNode ) ;
PHP_METHOD ( sequoiaGroup, createNode ) ;
PHP_METHOD ( sequoiaGroup, start ) ;
PHP_METHOD ( sequoiaGroup, stop ) ;
PHP_METHOD ( sequoiaGroup, isCatalog ) ;

/* ************* node ***************** */

PHP_METHOD ( sequoiaNode, stop ) ;
PHP_METHOD ( sequoiaNode, start ) ;
PHP_METHOD ( sequoiaNode, getNodeName ) ;
PHP_METHOD ( sequoiaNode, getServiceName ) ;
PHP_METHOD ( sequoiaNode, getHostName ) ;
PHP_METHOD ( sequoiaNode, getStatus ) ;
PHP_METHOD ( sequoiaNode, connect ) ;




#ifdef ZTS
#define SEQUOIADB_G(v) TSRMG(sqldb_globals_id, zend_sqldb_globals *, v)
#else
#define SEQUOIADB_G(v) (sqldb_globals.v)
#endif

#endif
