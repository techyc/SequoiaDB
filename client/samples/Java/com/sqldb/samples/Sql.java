package com.sqldb.samples;

/******************************************************************************
 *
 * Name: Sql.java
 * Description: This program demonstrates how to use the Java Driver to
 *				manipulate DB by SQL 
 *				Get more details in API document
 * 
 * ****************************************************************************/

import com.sqldb.base.DBCursor;
import com.sqldb.base.Sequoiadb;
import com.sqldb.exception.BaseException;

public class Sql {
	public static void main(String[] args) {
		if (args.length != 1) {
			if (args.length != 1) {
				System.out
						.println("Please give the database server address <IP:Port>");
				System.exit(0);
			}
		}
		// the database server address
		String connString = args[0];
		Sequoiadb sdb = null;

		try {
			sdb = new Sequoiadb(connString, "", "");
		} catch (BaseException e) {
			System.out.println("Failed to connect to database: " + connString
					+ ", error description" + e.getErrorType());
			e.printStackTrace();
			System.exit(1);
		}
		try {
			String csFullName = Constants.SQL_CS_NAME + "." + Constants.SQL_CL_NAME;
			String sql = "";
			// create collectionspace
			sql = "create collectionspace " + Constants.SQL_CS_NAME;
			sdb.exec(sql);
			// create table
			sql = "create collection " + csFullName;
			sdb.execUpdate(sql);
			// insert data into table
			sql = "insert into " + csFullName + " (a,b,c)" + " values(1,\"John\",20)";
			sdb.execUpdate(sql);
			// select from table
			sql = "select * from " + csFullName;
			DBCursor cursor = sdb.exec(sql);
			while(cursor.hasNext())
				System.out.println(cursor.getNext());
		} catch (BaseException e) {
			e.printStackTrace();
		}
	}

}
