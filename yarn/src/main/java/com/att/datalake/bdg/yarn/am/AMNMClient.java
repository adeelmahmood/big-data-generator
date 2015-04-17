package com.att.datalake.bdg.yarn.am;

import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AMNMClient {

	private static final Logger log = LoggerFactory.getLogger(AMNMClient.class);

	private final NMClientAsync client;
	private final org.apache.hadoop.conf.Configuration conf;

	@Autowired
	public AMNMClient(NMCallbackHandler handler, org.apache.hadoop.conf.Configuration conf) {
		client = new NMClientAsyncImpl(handler);
		this.conf = conf;
	}

	public void init() {
		client.init(conf);
		client.start();
		log.info("AM to NM connection established");
	}

	public void stop() {
		client.stop();
	}
}
