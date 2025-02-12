package com.sqldb.samples;

/******************************************************************************
 * 
 * Name: Snapshot.java 
 * Description: This program demonstrates how to use the Java Driver to get
 * 				database snapshot ( for other types of * snapshots/lists,
 *				the steps are very similar )
 *				Get more details in API document
 * 
 ******************************************************************************/
import java.io.IOException;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.sqldb.base.DBCursor;
import com.sqldb.base.Sequoiadb;
import com.sqldb.exception.BaseException;

public class Snapshot {
	public static void main(String[] args) throws IOException {
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
		
		BSONObject matcher = new BasicBSONObject();
		matcher.put("Name", "SAMPLE.employee");
		DBCursor cursor = sdb.getSnapshot(Sequoiadb.SDB_SNAP_COLLECTIONS,
				matcher, null, null);
		while (cursor.hasNext())
			System.out.println(cursor.getNext());

		sdb.disconnect();
	}
}
