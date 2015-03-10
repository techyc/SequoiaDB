/**
 *      Copyright (C) 2012 SqlDB Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
/**
 * @package com.sqldb.base;
 * @brief SqlDB DataSource
 * @author tanzhaobo
 */
package com.sqldb.base;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import com.sqldb.exception.BaseException;
import com.sqldb.net.ConfigOptions;

/**
 * @class SequoiadbDatasource
 * @brief SqlDB DataSource
 */
public class SequoiadbDatasource
{
	private volatile LinkedList<Sequoiadb> idle_sqldbs = new LinkedList<Sequoiadb>();
	private volatile  LinkedList<Sequoiadb> used_sqldbs = new LinkedList<Sequoiadb>(); 
	private volatile ArrayList<String> normal_urls = new ArrayList<String>();
	private volatile ArrayList<String> abnormal_urls = new ArrayList<String>();
	private String username = null;
	private String password = null;
	private ConfigOptions nwOpt = null;
	private SequoiadbOption dsOpt = null;
	private Timer timer = new Timer(true);
	private Timer timer2 = new Timer(true);
	private final double MULTIPLE = 1.2;
	private final double MULTIPLE2 = 0.8;
	private Random rand = new Random(47);
	
	/**
	 * @fn int getIdleConnNum()
	 * @brief Get the current idle connection amount.
	 */
	public synchronized int getIdleConnNum()
	{
		return idle_sqldbs.size();
	}
	
	/**
	 * @fn int getUsedConnNum()
	 * @brief Get the current used connection amount.
	 */
	public synchronized int getUsedConnNum()
	{
		return used_sqldbs.size();
	}
	
	/**
	 * @fn int getNormalAddrNum()
	 * @brief Get the current normal address amount.
	 */
	public synchronized int getNormalAddrNum()
	{
		return normal_urls.size();
	}
	
	/**
	 * @fn int getAbnormalAddrNum()
	 * @brief Get the current abnormal address amount.
	 */
	public synchronized int getAbnormalAddrNum()
	{
		return abnormal_urls.size();
	}
	
	public synchronized void addCoord(String url) 
	{
	    normal_urls.add(url);
	}
	
	/**
	 * @fn SequoiadbDatasource(ArrayList<String> urls, String username, String password,
	 *		                   ConfigOptions nwOpt, SequoiadbOption dsOpt)
	 * @brief constructor.
	 * @param urls the addresses of coords, can't be null or empty,
	 *        e.g."ubuntu1:11810","ubuntu2:11810",...
	 * @param username the username for logging sqldb
	 * @param password the password for logging sqldb
	 * @param nwOpt the options for connection
	 * @param dsOpt the options for datasource  
	 * @note When offer several addresses for datasource to use, if 
	 *       some of them are not available(invalid address, network error, coord shutdown,
	 *       catalog replica group is not available), we will put these addresses
	 *       into a queue, and check them periodically. If some of them is valid again,
	 *       get them back for use. When datasource get a unavailable address to connect,
	 *       the default timeout time is 100ms, and default retry time is 0, use nwOpt can change them.
	 * @see ConfigOptions
	 * @see SequoiadbOption
	 * @exception com.sqldb.exception.BaseException
	 */
	public SequoiadbDatasource(List<String> urls, String username, String password,
			ConfigOptions nwOpt, SequoiadbOption dsOpt) throws BaseException
	{
		if (null == urls || 0 == urls.size())
			throw new BaseException("SDB_INVALIDARG", urls);

		ConfigOptions temp = new ConfigOptions();
		temp.setConnectTimeout(100);
		temp.setMaxAutoConnectRetryTime(0);
		this.username = (null == username) ? "" : username;
		this.password = (null == password) ? "" : password;
		this.nwOpt = (null == nwOpt) ? temp : nwOpt;
		this.dsOpt = (null == dsOpt) ? new SequoiadbOption() : dsOpt;

		this.normal_urls.addAll(urls);

		try
		{
			init(this.dsOpt);
		}
		catch(BaseException e)
		{
			throw e;
		}
		timer.schedule(new CleanConnectionTask(this), this.dsOpt.getRecheckCyclePeriod(),
				       this.dsOpt.getRecheckCyclePeriod());
		timer2.schedule(new RecaptureCoordAddrTask(this), this.dsOpt.getRecaptureConnPeriod(),
				        this.dsOpt.getRecaptureConnPeriod());
	}

	/**
	 * @fn SequoiadbDatasource(String url, String username, String password,
	 *		                   SequoiadbOption dsOpt)
	 * @brief Constructor.
	 * @param url the address of coord, can't be null, e.g."ubuntu1:11810"
	 * @param username the username of sqldb
	 * @param password the password of sqldb
	 * @param dsOpt the option of datasource
	 * @exception com.sqldb.exception.BaseException
	 */
	public SequoiadbDatasource(String url, String username, String password,
			SequoiadbOption dsOpt) throws BaseException
	{
		if (null == url)
			throw new BaseException("SDB_INVALIDARG", url);
		this.normal_urls.add(url);

		ConfigOptions temp = new ConfigOptions();
		temp.setConnectTimeout(100);
		temp.setMaxAutoConnectRetryTime(0);
		this.username = (null == username) ? "" : username;
		this.password = (null == password) ? "" : password;
		this.nwOpt = (null == nwOpt) ? temp : nwOpt;
		this.dsOpt = (null == dsOpt) ? new SequoiadbOption() : dsOpt;
		
		try
		{
			init(this.dsOpt);
		}
		catch(BaseException e)
		{
			throw e;
		}
		timer.schedule(new CleanConnectionTask(this), dsOpt.getRecheckCyclePeriod(), dsOpt.getRecheckCyclePeriod());
	}	
	
	/**
	 * @fn void init(SequoiadbOption dsOpt)
	 * @brief Initialize datasource.
	 * @param dsOpt the configuration for datasource
	 * @exception com.sqldb.Exception.BaseException              
	 */
	private void init(SequoiadbOption dsOpt) throws BaseException
	{
		if (dsOpt.getMaxConnectionNum() < 0)
			throw new BaseException("SDB_INVALIDARG",
					                "maxConnectionNum is negative: " + dsOpt.getMaxConnectionNum());
		if (dsOpt.getMaxConnectionNum() == 0)
			return ;
		if (dsOpt.getMaxConnectionNum() < dsOpt.getInitConnectionNum())
			throw new BaseException("SDB_INVALIDARG", "maxConnectionNum is less than initConnectionNum, maxConnectionNum is " +
					dsOpt.getMaxConnectionNum() + ", initConnectionNum is " + dsOpt.getInitConnectionNum());
		if (dsOpt.getInitConnectionNum() < 0)
			throw new BaseException("SDB_INVALIDARG", 
					            "initConnectionNum is negative: " + dsOpt.getInitConnectionNum());
		increaseConn(dsOpt.getInitConnectionNum());
	}

	/**
	 * @fn String getCoordAddr()
	 * @brief Get coord url.
	 * @return one of the coord addresses
	 * @exception com.sqldb.Exception.BaseException              
	 */
	private synchronized String getCoordAddr() throws BaseException
	{
		if (1 < normal_urls.size())
			return normal_urls.get(rand.nextInt(normal_urls.size()));
		else if (1 == normal_urls.size())
			return normal_urls.get(0);
		else
			throw new BaseException("SDB_INVALIDARG", "no available address");
	}
	
	/**
	 * @fn String getBackCoordAddr()
	 * @brief Get back all the available coord addresses periodically.
	 * @exception com.sqldb.Exception.BaseException              
	 */
    synchronized void getBackCoordAddr()
	{
		Sequoiadb sdb = null;
		String addr = null;
		
		if (0 == abnormal_urls.size())
			return;
		for (int i = 0; i < abnormal_urls.size(); i++ )
		{
			addr = abnormal_urls.get(i);
			try
			{
				sdb = new Sequoiadb(addr, this.username, this.password, this.nwOpt);
				if (sdb.isValid()) // it seem no need to check here?
				{
					normal_urls.add(addr);
					abnormal_urls.remove(addr);
					i--;
				}
			}
			catch(BaseException e)
			{
			}
		}
	}
	
	/**
	 * @fn Sequoiadb getConnDrt()
	 * @brief Get a connection directly.
	 * @exception com.sqldb.Exception.BaseException              
	 */
    private synchronized Sequoiadb getConnDrt() throws BaseException
	{
    	Sequoiadb sdb = null;
		String addr = getCoordAddr();
		
		while (normal_urls.size() > 0)
		{
			try
			{
				sdb = new Sequoiadb(addr, this.username, this.password, this.nwOpt);
				break;
			}
			catch (BaseException e)
			{
				String errType = e.getErrorType();
				if (errType.equals("SDB_NETWORK") || errType.equals("SDB_INVALIDARG") ||
					errType.equals("SDB_NET_CANNOT_CONNECT"))
				{
					abnormal_urls.add(addr);
					normal_urls.remove(addr);
					addr = getCoordAddr();
					continue;
				}
				else
					throw e;
			}
		}

		if (null == sdb)
			throw new BaseException("SDB_INVALIDARG");
		return sdb;
	}
    
	/**
	 * @fn Sequoiadb getConnection()
	 * @brief  Get the connection from current datasource.
	 * @return Sequoiadb the connection for use
	 * @exception com.sqldb.Exception.BaseException
	 *            when datasource run out, throws BaseException with the type of "SDB_DRIVER_DS_RUNOUT"
	 * @exception InterruptedException
	 */
	public synchronized Sequoiadb getConnection() throws BaseException, InterruptedException
	{
		if (dsOpt.getMaxConnectionNum() == 0)
			return getConnDrt();
		Sequoiadb sdb = null;
		if ((idle_sqldbs.size() > 0) && (used_sqldbs.size() < dsOpt.getMaxConnectionNum())) 
		{
			sdb = idle_sqldbs.poll();
			while((null != sdb) && (!sdb.isValid()))
			{
				sdb = idle_sqldbs.poll();
			}
			if (null == sdb)
				sdb = getConnDrt();
			used_sqldbs.add(sdb);
		}
		else
		{
			if (used_sqldbs.size() >= dsOpt.getMaxConnectionNum())
			{
				wait(dsOpt.getTimeout());
				if (used_sqldbs.size() >= dsOpt.getMaxConnectionNum())
					throw new BaseException("SDB_DRIVER_DS_RUNOUT");
			}
			sdb = getConnDrt();
			used_sqldbs.add(sdb);
			Thread t = new Thread(new CreateConnectionTask(this));
			t.start();
		}
		return sdb;
	}
	
	/**
	 * @fn void close(Sequoiadb sdb)
	 * @brief Put the connection back to the datasource.
	 *        In the case of the connecton is not belong to current datasource or
	 *        the connecton is outdate(by default, if a connection has not be used for 10 minutes),
	 *        this API will disconnect directly.
	 * @param sdb the connection get from current datasource, can't be null 
	 * @exception com.sqldb.Exception.BaseException
	 *        if param sdb is null, throws BaseException with the type "SDB_INVALIDARG"
	 */
	public synchronized void close(Sequoiadb sdb) throws BaseException
	{
		if (null == sdb)
			throw new BaseException("SDB_INVALIDARG", sdb);
		if (dsOpt.getAbandonTime() <= 0)
			throw new BaseException("SDB_INVALIDARG", "abandonTime is negative: " + dsOpt.getAbandonTime());
		if (dsOpt.getRecheckCyclePeriod() <= 0)
			throw new BaseException("SDB_INVALIDARG", "recheckCyclePeriod is negative: " + dsOpt.getRecheckCyclePeriod());
		if (dsOpt.getRecheckCyclePeriod() >= dsOpt.getAbandonTime())
			throw new BaseException("SDB_INVALIDARG", "recheckCyclePeriod is not less than abandonTime, recheckCyclePeriod is " +
					dsOpt.getRecheckCyclePeriod() + ", abandonTime is " + dsOpt.getAbandonTime());
		if (used_sqldbs.contains(sdb)) 
		{
			used_sqldbs.remove(sdb);
			if (dsOpt.getMaxConnectionNum() == 0)
			{
				sdb.disconnect();
				return;
			}
			long lastTime = sdb.getConnection().getLastUseTime();
			long currentTime = System.currentTimeMillis();
			if (currentTime - lastTime + MULTIPLE * dsOpt.getRecheckCyclePeriod() >= dsOpt.getAbandonTime())
			{
				sdb.disconnect();
				return;
			}
			else
			{
				sdb.closeAllCursors();
				sdb.releaseResource();
				idle_sqldbs.add(sdb);
				notify();	
			}
		}
		else
		{
			sdb.disconnect();
			return;
		}
	}
	
	/**
	 * @fn void increaseConnetions()
	 * @bref Add another 20 Sequoiadb objects to the datasource. 
	 */
	synchronized void increaseConnetions() throws BaseException
	{
		if (dsOpt.getMaxConnectionNum() == 0)
			return;
		if (idle_sqldbs.size() >= dsOpt.getMaxIdeNum())
			return;
		if (dsOpt.getDeltaIncCount() < 0)
				throw new BaseException("SDB_INVALIDARG", 
						                "deltaIncCount is negative: " + dsOpt.getDeltaIncCount());
		if (dsOpt.getMaxConnectionNum() < dsOpt.getDeltaIncCount())
			throw new BaseException("SDB_INVALIDARG",
					                "deltaIncCount is greater than maxConnectionNum, deltaIncCount is " +
					dsOpt.getDeltaIncCount() + ", maxConnectionNum is " + dsOpt.getMaxConnectionNum());
		if (used_sqldbs.size() < dsOpt.getMaxConnectionNum() - dsOpt.getDeltaIncCount())
			increaseConn(dsOpt.getDeltaIncCount());
		else
			increaseConn(dsOpt.getMaxConnectionNum() - used_sqldbs.size());
	}
	
	/**
	 * @fn void cleanAbandonConnection()
	 * @brief  clean the tiemout connection periodically
	 */
	synchronized void cleanAbandonConnection() throws BaseException
	{
		if ((0 == idle_sqldbs.size()) && (0 == used_sqldbs.size()))
			return ;
		if (dsOpt.getAbandonTime() <= 0)
			throw new BaseException("SDB_INVALIDARG", "abandonTime is negative: " + dsOpt.getAbandonTime());
		if (dsOpt.getRecheckCyclePeriod() <= 0)
			throw new BaseException("SDB_INVALIDARG", "recheckCyclePeriod is negative: " + dsOpt.getRecheckCyclePeriod());
		if (dsOpt.getRecheckCyclePeriod() >= dsOpt.getAbandonTime())
			throw new BaseException("SDB_INVALIDARG", "recheckCyclePeriod is not less than abandonTime, recheckCyclePeriod is " +
					dsOpt.getRecheckCyclePeriod() + ", abandonTime is " + dsOpt.getAbandonTime());
		
		long lastTime = 0;
		long currentTime = System.currentTimeMillis();
		ListIterator<Sequoiadb> list1 = idle_sqldbs.listIterator(0);
		while(list1.hasNext())
		{
			Sequoiadb db = list1.next();
			lastTime = db.getConnection().getLastUseTime(); 
			if (currentTime - lastTime + MULTIPLE * dsOpt.getRecheckCyclePeriod() >= dsOpt.getAbandonTime())
			{
				db.disconnect () ;
				list1.remove();
			}
		}
		for (int i = 0; i < idle_sqldbs.size() - dsOpt.getMaxIdeNum(); i++)
		{
			Sequoiadb db = idle_sqldbs.poll();
			db.disconnect();
			i--;
		}

	}
	
	/**
	 * @fn void increaseConn(int num)
	 * @brief Increase connections.
	 */
	private synchronized void increaseConn(int num)
	{
		if (num < 0)
			throw new BaseException("SDB_INVALIDARG");
		for (int i = 0; i < num; i++)
		{
			String coordAddr = getCoordAddr();
			Sequoiadb sdb = null;
			try
			{
				sdb = new Sequoiadb(coordAddr, username, password, this.nwOpt);
			}
			catch(BaseException e)
			{
				String errType = e.getErrorType();
				if (errType.equals("SDB_NETWORK") || errType.equals("SDB_INVALIDARG") ||
					errType.equals("SDB_NET_CANNOT_CONNECT"))
				{
					abnormal_urls.add(coordAddr);
					normal_urls.remove(coordAddr);
					i--;
					continue;
				}
				else
					throw e;
			}
			idle_sqldbs.add(sdb);
		}
	}
}

class CleanConnectionTask extends TimerTask
{
	private SequoiadbDatasource datasource;
	
	public CleanConnectionTask(SequoiadbDatasource ds)
	{
		datasource = ds;
	}
	@Override
	public void run()
	{
		datasource.cleanAbandonConnection();
	}
}

class CreateConnectionTask implements Runnable
{
	private SequoiadbDatasource datasource;
	
	public CreateConnectionTask(SequoiadbDatasource ds)
	{
		datasource = ds;
	}
	public void run()
	{
		datasource.increaseConnetions();
	}
}

class RecaptureCoordAddrTask extends TimerTask
{
	private SequoiadbDatasource datasource;
	public RecaptureCoordAddrTask(SequoiadbDatasource ds){
		datasource = ds;
	}
	@Override
	public void run()
	{
		datasource.getBackCoordAddr();
	}
}
