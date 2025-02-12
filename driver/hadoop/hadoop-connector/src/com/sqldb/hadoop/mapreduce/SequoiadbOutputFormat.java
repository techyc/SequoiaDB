package com.sqldb.hadoop.mapreduce;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import com.sqldb.hadoop.io.SequoiadbWriter;
import com.sqldb.hadoop.util.SdbConnAddr;
import com.sqldb.hadoop.util.SequoiadbConfigUtil;

public class SequoiadbOutputFormat extends OutputFormat implements Configurable {
	private static final Log log = LogFactory
			.getLog(SequoiadbInputFormat.class);
	private static final int BULKINSERTNUMBER = 512;

	private String collectionSpaceName;
	private String collectionName;
	private int bulkNum;
	private SdbConnAddr[] sdbConnAddr;
	private String user;
	private String passwd;

	public SequoiadbOutputFormat() {

	}

	@Override
	public void checkOutputSpecs(JobContext jobContext) {

	}

	@Override
	public OutputCommitter getOutputCommitter(
			TaskAttemptContext taskAttemptContext) throws IOException,
			InterruptedException {
		System.out.println("OutputCommitter");
		return new SequoiadbOutputCommitter();
	}

	@Override
	public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext)
			throws IOException, InterruptedException {

		InetAddress localAddr = null;
		try {
			localAddr = InetAddress.getLocalHost();
			log.debug(localAddr.getHostAddress());
		} catch (UnknownHostException e) {
			log.error(e.getMessage());
		}

		ArrayList<SdbConnAddr> localAddrList = new ArrayList<SdbConnAddr>();
		for (int i = 0; i < sdbConnAddr.length; i++) {
			if (sdbConnAddr[i].getHost().equals(localAddr.getHostAddress())
					|| sdbConnAddr[i].getHost().equals(localAddr.getHostName())) {
				localAddrList.add(sdbConnAddr[i]);
			}
		}

		if (localAddrList.isEmpty()) {
			for (int i = 0; i < sdbConnAddr.length; i++) {
				localAddrList.add(sdbConnAddr[i]);
			}
		}

		int i = 0;
		if (localAddrList.size() > 1) {
			Random rand = new Random();
			i = rand.nextInt(localAddrList.size());
		}

		log.debug("Select coord address:" + localAddrList.get(i).toString());

		return new SequoiadbWriter(collectionSpaceName, collectionName,
				localAddrList.get(i), user, passwd, bulkNum, "bulkinsert");
	}

	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration configuration) {
		this.conf = configuration;
		this.collectionName = SequoiadbConfigUtil.getOutCollectionName(conf);
		this.collectionSpaceName = SequoiadbConfigUtil
				.getOutCollectionSpaceName(conf);
		String bulkNumStr = SequoiadbConfigUtil.getOutputBulknum(conf);
		this.user = SequoiadbConfigUtil.getOutputUser(conf);
		this.passwd = SequoiadbConfigUtil.getOutputPasswd(conf);
		
		if (bulkNumStr != null) {
			try{
				this.bulkNum = Integer.valueOf(bulkNumStr);
			}catch( Exception e ){
				log.warn(e.toString());
				log.warn("bulkNum use " + BULKINSERTNUMBER);
				this.bulkNum = BULKINSERTNUMBER;
			}
		}else{
			this.bulkNum = BULKINSERTNUMBER;
		}

		String urlStr = SequoiadbConfigUtil.getOutputURL(conf);
		if (urlStr == null) {
			throw new IllegalArgumentException("The argument "
					+ SequoiadbConfigUtil.JOB_OUTPUT_URL + " must be set.");
		}
		
		sdbConnAddr = SequoiadbConfigUtil.getAddrList(urlStr);
		if (sdbConnAddr == null || sdbConnAddr.length == 0) {
			throw new IllegalArgumentException("The argument "
					+ SequoiadbConfigUtil.JOB_OUTPUT_URL + " must be set.");
		}
	}

}
