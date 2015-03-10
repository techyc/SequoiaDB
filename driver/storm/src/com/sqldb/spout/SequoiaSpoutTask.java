package com.sqldb.spout;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.bson.BSONObject;

import com.sqldb.base.CollectionSpace;
import com.sqldb.base.DBCollection;
import com.sqldb.base.DBCursor;
import com.sqldb.base.Sequoiadb;
import com.sqldb.exception.BaseException;

public class SequoiaSpoutTask implements Callable<Boolean>, Serializable,
		Runnable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1481907266524773498L;
	private static Logger LOG = Logger.getLogger(SequoiaSpoutTask.class);

	private LinkedBlockingQueue<BSONObject> queue;
	private Sequoiadb sdb;
	private CollectionSpace space;
	private DBCollection collection;
	private DBCursor cursor;
	private String[] collectionNames;
	private BSONObject query;
	private BSONObject selector;

	private AtomicBoolean running = new AtomicBoolean(true);

	/**
	 * @fn SequoiaSpoutTask(LinkedBlockingQueue<BSONObject> queue, String host,
	 *     int port, String userName, String password, String dbName, String[]
	 *     collectionNames, BSONObject query)
	 * @brief Constructor
	 * @param queue
	 *            The queue for push object
	 * @param host
	 *            The host name.
	 * @param port
	 *            The port for sqldb's coord node.
	 * @param userName
	 *            The user name for sqldb
	 * @param password
	 *            The password for sqldb
	 * @param dbName
	 *            The Collection space name for sqldb
	 * @param collectionNames
	 *            The collection name list
	 * @param query
	 *            The query condition
	 */
	public SequoiaSpoutTask(LinkedBlockingQueue<BSONObject> queue, String host,
			int port, String userName, String password, String dbName,
			String[] collectionNames, BSONObject query, BSONObject selector) {
		this.queue = queue;
		this.collectionNames = collectionNames;
		this.query = query;
		this.selector = selector;

		initSequoia(host, port, userName, password, dbName);
	}

	/**
	 * @fn initSequoia(String host, int port, String userName, String password,
	 *     String dbName)
	 * @brief connect to sqldb
	 * @param host
	 *            The host name.
	 * @param port
	 *            The port for sqldb's coord node.
	 * @param userName
	 *            The user name for sqldb
	 * @param password
	 *            The password for sqldb
	 * @param dbName
	 *            The Collection space name for sqldb
	 */
	private void initSequoia(String host, int port, String userName,
			String password, String dbName) {
		try {
			Sequoiadb sdb = new Sequoiadb(host, port, userName, password);
			space = sdb.getCollectionSpace(dbName);
		} catch (BaseException e) {
			LOG.error("SqlDB Exception:", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * @fn stopThread()
	 * @brief Stop query thread
	 */
	public void stopThread() {
		running.set(false);
	}

	@Override
	public void run() {
		try {
			call();
		} catch (Exception e) {
			LOG.error(e);
		}
	}

	@Override
	public Boolean call() throws Exception {
		try {
			String collectionName = this.collectionNames[0];
			this.collection = space.getCollection(collectionName);
			cursor = this.collection.query(query, selector, null, null);

			while (running.get()) {
				if (cursor.hasNext()) {
					BSONObject obj = cursor.getNext();
					if (LOG.isInfoEnabled()) {
						LOG.info("Fetching a new item from Sequoiadb cursor:"
								+ obj.toString());
					}

					queue.put(obj);
				} else {
					Thread.sleep(50);
				}
			}

		} catch (Exception e) {
			LOG.error("Failed to fetch record from sqldb, exception:" + e);
			if (running.get()) {
				throw new RuntimeException(e);
			}
		} finally {
			cursor.close();
			sdb.disconnect();
		}

		return true;
	}
}
