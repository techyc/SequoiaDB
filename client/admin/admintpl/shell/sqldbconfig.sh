#!/bin/bash

#部署错误是否需要回滚[1:是,0:否]
IS_ROLLBACK=0

#需要回滚的路径(不需要填写)
DELETE_PATH_ARR=()

#需要回滚的任务(不需要填写)
REVOKE_TASK_ARR=()

#是否输出调试信息[1:输出调试信息,2:输出普通信息,3:不输出]
IS_PRINGT_DEBUG=1

#安装文件的路径
INSTALL_PATH="/home/sqldb/www/http/shell"

#安装文件的文件名
INSTALL_NAME="sqldb-1.5-linux_x86_64-installer.run"

#sdbcm端口
SDBCM_PORT="60010"

#分区组列表
LIST_GROUP=("g1" "g2" "g3")

#主机列表
LIST_HOST=(\
"ARRAY_HOST_1" \
"ARRAY_HOST_2" \
"ARRAY_HOST_3" \
"ARRAY_HOST_4" \
)

ARRAY_HOST_1=("ubuntu-test-01" "/opt/sqldb" "sdbadmin_group" "sdbadmin" "sqldb")
ARRAY_HOST_2=("ubuntu-test-02" "/opt/sqldb" "sdbadmin_group" "sdbadmin" "sqldb")
ARRAY_HOST_3=("ubuntu-test-03" "/opt/sqldb" "sdbadmin_group" "sdbadmin" "sqldb")
ARRAY_HOST_4=("ubuntu-test-04" "/opt/sqldb" "sdbadmin_group" "sdbadmin" "sqldb")

#节点列表
LIST_NODE=(\
"ARRAY_NODE_1" \
"ARRAY_NODE_2" \
"ARRAY_NODE_3" \
"ARRAY_NODE_4" \
"ARRAY_NODE_5" \
"ARRAY_NODE_6" \
"ARRAY_NODE_7" \
"ARRAY_NODE_8" \
"ARRAY_NODE_9" \
"ARRAY_NODE_10" \
"ARRAY_NODE_11" \
"ARRAY_NODE_12" \
)

ARRAY_NODE_1=("coord" "" "ARRAY_HOST_1" "SDBCONF_1")
ARRAY_NODE_2=("coord" "" "ARRAY_HOST_2" "SDBCONF_2")
ARRAY_NODE_3=("coord" "" "ARRAY_HOST_3" "SDBCONF_3")
ARRAY_NODE_4=("coord" "" "ARRAY_HOST_4" "SDBCONF_4")
ARRAY_NODE_5=("cata" "" "ARRAY_HOST_1" "SDBCONF_5")
ARRAY_NODE_6=("cata" "" "ARRAY_HOST_2" "SDBCONF_6")
ARRAY_NODE_7=("cata" "" "ARRAY_HOST_3" "SDBCONF_7")
ARRAY_NODE_8=("cata" "" "ARRAY_HOST_4" "SDBCONF_8")
ARRAY_NODE_9=("data" "g1" "ARRAY_HOST_1" "SDBCONF_9")
ARRAY_NODE_10=("data" "g1" "ARRAY_HOST_2" "SDBCONF_10")
ARRAY_NODE_11=("data" "g1" "ARRAY_HOST_3" "SDBCONF_11")
ARRAY_NODE_12=("data" "g1" "ARRAY_HOST_4" "SDBCONF_12")

#配置文件参数表
SDB_CONFIG=(confpath logpath diagpath dbpath indexpath bkuppath maxpool svcname replname shardname catalogname httpname diaglevel role catalogaddr logfilesz logfilenum transactionon numpreload maxprefpool maxsubquery logbuffsize)
SDBCONF_1=("" "" "" "/opt/sqldb/database/coord/11850" "" "" "0" "11850" "" "" "" "" "3" "" "" "64" "20" "false" "0" "200" "10" "1024")
SDBCONF_2=("" "" "" "/opt/sqldb/database/coord/11850" "" "" "0" "11850" "" "" "" "" "3" "" "" "64" "20" "false" "0" "200" "10" "1024")
SDBCONF_3=("" "" "" "/opt/sqldb/database/coord/11850" "" "" "0" "11850" "" "" "" "" "3" "" "" "64" "20" "false" "0" "200" "10" "1024")
SDBCONF_4=("" "" "" "/opt/sqldb/database/coord/11850" "" "" "0" "11850" "" "" "" "" "3" "" "" "64" "20" "false" "0" "200" "10" "1024")
SDBCONF_5=("" "" "" "/opt/sqldb/database/cata/11840" "" "" "0" "11840" "" "" "" "" "3" "" "" "64" "20" "false" "0" "200" "10" "1024")
SDBCONF_6=("" "" "" "/opt/sqldb/database/cata/11840" "" "" "0" "11840" "" "" "" "" "3" "" "" "64" "20" "false" "0" "200" "10" "1024")
SDBCONF_7=("" "" "" "/opt/sqldb/database/cata/11840" "" "" "0" "11840" "" "" "" "" "3" "" "" "64" "20" "false" "0" "200" "10" "1024")
SDBCONF_8=("" "" "" "/opt/sqldb/database/cata/11840" "" "" "0" "11840" "" "" "" "" "3" "" "" "64" "20" "false" "0" "200" "10" "1024")
SDBCONF_9=("" "" "" "/opt/sqldb/database/data/11830" "" "" "0" "11830" "" "" "" "" "3" "" "" "64" "20" "false" "0" "200" "10" "1024")
SDBCONF_10=("" "" "" "/opt/sqldb/database/data/11830" "" "" "0" "11830" "" "" "" "" "3" "" "" "64" "20" "false" "0" "200" "10" "1024")
SDBCONF_11=("" "" "" "/opt/sqldb/database/data/11830" "" "" "0" "11830" "" "" "" "" "3" "" "" "64" "20" "false" "0" "200" "10" "1024")
SDBCONF_12=("" "" "" "/opt/sqldb/database/data/11830" "" "" "0" "11830" "" "" "" "" "3" "" "" "64" "20" "false" "0" "200" "10" "1024")
