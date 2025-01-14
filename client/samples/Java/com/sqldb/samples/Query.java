package com.sqldb.samples;

/******************************************************************************
 *
 * Name: Query.java
 * Description: This program demonstrates how to use the Java Driver to
 *				query data from DB
 *				Get more details in API document
 * 
 * ****************************************************************************/

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.sqldb.base.CollectionSpace;
import com.sqldb.base.DBCollection;
import com.sqldb.base.DBCursor;
import com.sqldb.base.Sequoiadb;
import com.sqldb.exception.BaseException;

public class Query {
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out
					.println("Please give the database server address <IP:Port>");
			System.exit(0);
		}

		// the database server address
		String connString = args[0];
		Sequoiadb sdb = null;
		CollectionSpace cs = null;
		DBCollection cl = null;

		try {
			sdb = new Sequoiadb(connString, "", "");
		} catch (BaseException e) {
			System.out.println("Failed to connect to database: " + connString
					+ ", error description" + e.getErrorType());
			e.printStackTrace();
			System.exit(1);
		}

		if (sdb.isCollectionSpaceExist(Constants.CS_NAME))
			cs = sdb.getCollectionSpace(Constants.CS_NAME);
		else
			cs = sdb.createCollectionSpace(Constants.CS_NAME);

		if (cs.isCollectionExist(Constants.CL_NAME))
			cl = cs.getCollection(Constants.CL_NAME);
		else
			cl = cs.createCollection(Constants.CL_NAME);

		try {
			BSONObject index = null;
			DBCursor indexCursor = cl.getIndex(Constants.INDEX_NAME);
			if (indexCursor.hasNext())
				index = indexCursor.getNext();
			// result cursor
			DBCursor dataCursor = null;
			// query condition
			BSONObject query = new BasicBSONObject();
			BSONObject condition = new BasicBSONObject();
			condition.put("$gte", 0);
			condition.put("$lte", 9);
			query.put("Id", condition);
			// return fields
			BSONObject selector = new BasicBSONObject();
			selector.put("Id", null);
			selector.put("Age", null);
			// order by ASC(1)/DESC(-1)
			BSONObject orderBy = new BasicBSONObject();
			orderBy.put("Id", -1);
			if (index == null)
				dataCursor = cl.query(query, selector, orderBy, null, 0, -1);
				// or
				// dataCursor = cl.query("{'Id':{'$gte':0,'$lte':9}}", "{'Id':null,'Age':null}", "{'Id':-1}", null, 0, -1);
			else
				dataCursor = cl.query(query, selector, orderBy, index, 0, -1);
			// or
			// dataCursor = cl.query("{'Id':{'$gte':0,'$lte':9}}", "{'Id':null,'Age':null}", "{'Id':-1}", index, 0, -1);
			
			// operate data by cursor
			while (dataCursor.hasNext()) {
				System.out.println(dataCursor.getNext());
			}
			// get count by match condition
			long count = cl.getCount(query);
			System.out.println("Get count by condition: " + query
					+ ", count = " + count);

			// see more details about query function in API document
		} catch (BaseException e) {
			e.printStackTrace();
		}

		sdb.disconnect();
	}
}
