SqlDB README
=================

Welcome to SqlDB!

SqlDB is a distributed document-oriented NoSQL Database.

Engine:
-----------------
	sqldb       -- SqlDB Engine
	sdbstart        -- SqlDB Engine start
	sdbstop         -- SqlDB Engine stop
	sdblist         -- SqlDB Engine list
	sdbfmp          -- SqlDB fenced mode process


Shell:
-----------------
	sdb             -- SqlDB client
	sdbbp           -- SqlDB client backend process


Cluster Manager:
-----------------
	sdbcm           -- SqlDB cluster manager
	sdbcmart        -- SqlDB cluster manager start
	sdbcmtop        -- SqlDB cluster manager stop
	sdbcmd          -- SqlDB cluster manager daemon


Tools:
-----------------
	sdbdpsdump      -- SqlDB log dump
	sdbexprt        -- SqlDB export
	sdbimprt        -- SqlDB import
	sdbinspt        -- SqlDB data inspection
	sdbrestore      -- SqlDB restore
	sdbtop          -- SqlDB TOP
	sdbperfcol      -- SqlDB performance collection
	sdbwsart        -- SqlDB web service start
	sdbwstop        -- SqlDB web service stop


Drivers:
-----------------
	C Driver:
		libsdbc.a
		libsdbc.so
	C++ Driver:
		libsdbcpp.a
		libsdbcpp.so
	PHP Driver:
		libsdbphp-x.x.x.so
	JAVA Driver:
		sqldb.jar
	PYTHON Driver:
		lib.linux-x86_64-2.6
	.NET Driver:
		sqldb.dll
	Python Driver:
		pysqldb.tar.gz


Connectors:
-----------------
	Hadoop Connector:
		hadoop-connector.jar
	Hive Connector:
		hive-sqldb-apache.jar
	Storm Connector:
		storm-sqldb.jar


Building Prerequisites:
-----------------
	scons ( 2.3.0 )
	ant ( 1.8.2 )
	Python ( 2.7.3 )
	PostgreSQL ( 9.3.4 )
	Linux x86-64:
		g++ ( 4.3.4 )
		gcc ( 4.3.4 )
		make ( 3.81 )
		kernel ( 3.0.13-0.27-default )
	Linux PPC64:
		g++ ( 4.3.4 )
		gcc ( 4.3.4 )
		make ( 3.81 )
		kernel ( 3.0.13-0.27-ppc64 )
	Windows:
		Windows SDK 7.1 ( Installation path must be C:\Program Files\Microsoft SDKs\Windows\v7.1 or
		C:\Program Files (x86)\Microsoft SDKs\Windows\v7.1 )
	Note:
		The utility and kernel version are for recommendation only.
		Users may use different version but may need to deal with any incompatibility issues.


Building Engine:
-----------------
	Engine Only:
		scons --engine
	C/C++ Client:
		scons --client
	Shell:
		scons --shell
	Tools:
		scons --tool
	Testcase:
		scons --testcase
	FMP:
		scons --fmp
	All ( except drivers ):
		scons --all
	Note:
		adding option "--dd" for debug build


Building Drivers:
-----------------
	C/C++ Client:
		scons --client
	PHP Client:
		cd driver/php5
		scons --phpversion=5.4.6
		Note:
			PHP source code is located in thirdparty/php directory
			The dir name must be "php-<version>"
	Python Client:
		<python-devel package is required>
		cd driver/python
		scons
	Java Client:
		cd driver/java
		scons
	.Net Client:
		cd driver/C#.Net
		scons


Building Connectors:
-----------------
	Hadoop Connector:
		cd driver/java
		scons
		cd driver/hadoop/hadoop-connector
		ant -Dhadoop.version=2.2
	Hive Connector:
		cd driver/java
		scons
		cd driver/hadoop/hive
		ant
	Storm Connector:
		cd driver/storm
		ant
	PostgreSQL FDW:
		cd driver/postgresql
		make local
		# Make sure pg_config is in PATH
		make install


Package RPM Prerequisites:
-----------------
        rpmbuild ( 4.8.0 )
        scons ( 2.3.0 )
        ant ( 1.8.2 )
        Python ( 2.7.3 )
        PostgreSQL ( 9.3.4 )
        Linux x86-64:
                g++ ( 4.3.4 )
                gcc ( 4.3.4 )
                make ( 3.81 )
                kernel ( 3.0.13-0.27-default )


Package RPM:
-----------------
        # root permission is required
        # for RHEL and CentOS only
        python script/package.py
        # the RPM-package will output in package/output/RPMS/


Running:
-----------------
	For command line options to start SqlDB, invoke:
		$ ./sdbstart --help
	For command line options to stop SqlDB, invoke:
		$ ./sdbstop --help
	For command line options to start cluster manager, invoke:
		$ ./sdbcmart --help
	For command line options to stop cluster manager, invoke:
		$ ./sdbcmtop --help


	To run in standalone mode:
		$ mkdir /sqldb/data
		$ cd /sqldb/data
		$ /opt/sqldb/bin/sdbstart -p 11810 --force
		$ # sqldb start successful
		$ # start sqldb shell
		$ /opt/sqldb/bin/sdb
		> var db = new Sdb() ;
		> db.help() ;


	To run in cluster mode, please refer SqlDB Information Center.


Documentation:
-----------------
[SqlDB Home Page](http://www.sqldb.com/)


Restrictions:
-----------------
	- SqlDB officially supports x86_64 and ppc64 Linux build on CentOS, Redhat, SUSE and Ubuntu.
	- Windows build and 32 bit build are for testing purpose only.


License:
-----------------
	Most SqlDB source files are made available under the terms of the
	GNU Affero General Public License (AGPL). See individual files for details.
	All source files for clients, drivers and connectors are released
	under Apache License v2.0.
