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
 * @brief SqlDB Driver for Java
 * @author Tanzhaobo
 */
package com.sqldb.base;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.sqldb.exception.BaseException;

/**
 * @class Domain
 * @brief Database operation interfaces of Sequoiadb domain.
 */
public class Domain {
	private String name;
	private Sequoiadb sqldb;

	/**
	 * @fn String getName()
	 * @brief Return the name of current domain.
	 * @return The name of current domain
	 */
	public String getName() {
		return name;
	}

	/**
	 * @fn Sequoiadb getSequoiadb()
	 * @brief Return the Sequoiadb connection instance of current domain belong to.
	 * @return Sequoiadb connection instance 
	 */
	public Sequoiadb getSequoiadb() {
		return sqldb;
	}

	/**
	 * @fn Domain(Sequoiadb sqldb, String name)
	 * @brief Constructor
	 * @param sqldb
	 *            Sequoiadb connection instance 
	 * @param name
	 *            the name for the created domain
	 */
	Domain(Sequoiadb sqldb, String name) {
		this.name = name;
		this.sqldb = sqldb;
	}
	
	/**
	 * @fn void alterDomain(BSONObject options)
	 * @brief Alter the current domain.
     * @param options the options user wants to alter:
     *<ul>
     *<li>Groups:    The list of replica groups' names which the domain is going to contain.
     *               eg: { "Groups": [ "group1", "group2", "group3" ] }, it means that domain
     *               changes to contain "group1", "group2" and "group3".
     *               We can add or remove groups in current domain. However, if a group has data
     *               in it, remove it out of domain will be failing.
     *<li>AutoSplit: Alter current domain to have the ability of automatically split or not. 
     *               If this option is set to be true, while creating collection(ShardingType is "hash") in this domain,
     *               the data of this collection will be split(hash split) into all the groups in this domain automatically.
     *               However, it won't automatically split data into those groups which were add into this domain later.
     *               eg: { "Groups": [ "group1", "group2", "group3" ], "AutoSplit: true" }
     *</ul>
	 * @exception com.sqldb.exception.BaseException
	 */
	public void alterDomain(BSONObject options) throws BaseException {
		if (null == options)
			throw new BaseException("SDB_INVALIDARG", options);
		BSONObject newObj = new BasicBSONObject();
		newObj.put(SequoiadbConstants.FIELD_NAME_NAME, this.name);
		newObj.put(SequoiadbConstants.FIELD_NAME_OPTIONS, options);
		SDBMessage rtn = this.sqldb.adminCommand(SequoiadbConstants.CMD_NAME_ALTER_DOMAIN,
				                      0, 0, 0, -1, newObj,
				                      null, null, null);
		int flags = rtn.getFlags();
		if (flags != 0) {
			throw new BaseException(flags);
		}
	}
	
	/**
	 * @fn DBCursor listCSInDomain()
	 * @brief List all the collection spaces in current domain.
	 * @return the cursor of result
	 * @exception com.sqldb.exception.BaseException
	 */
	public DBCursor listCSInDomain() throws BaseException {
		return listCSCL(Sequoiadb.SDB_LIST_CS_IN_DOMAIN);
	}
	
	/**
	 * @fn DBCursor listCLInDomain()
	 * @brief List all the collections in current domain.
	 * @return the cursor of result
	 * @exception com.sqldb.exception.BaseException
	 */
	public DBCursor listCLInDomain() throws BaseException {
		return listCSCL(Sequoiadb.SDB_LIST_CL_IN_DOMAIN);
	}
	
	private DBCursor listCSCL(int type) throws BaseException {
		BSONObject matcher = new BasicBSONObject();
		BSONObject selector = new BasicBSONObject();
		matcher.put(SequoiadbConstants.FIELD_NAME_DOMAIN, this.name);
		selector.put(SequoiadbConstants.FIELD_NAME_NAME, null);
		DBCursor cursor = this.sqldb.getList(type, matcher, selector, null);
		return cursor;
	}
	
}