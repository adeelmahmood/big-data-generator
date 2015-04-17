package com.att.datalake.bdg.yarn.am;

import java.io.IOException;

import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AMRMClient {

	private static final Logger log = LoggerFactory.getLogger(AMRMClient.class);

	private final AMRMClientAsync client;
	private final org.apache.hadoop.conf.Configuration conf;

	private int amRpcPort = -1;
	private String amTrackingUrl = "";

	@Autowired
	public AMRMClient(RMCallbackHandler handler, org.apache.hadoop.conf.Configuration conf) {
		client = AMRMClientAsync.createAMRMClientAsync(1000, handler);
		this.conf = conf;
	}

	public void init() {
		client.init(conf);
		client.start();
		log.info("AM to RM connection established");
	}

	public RegisterApplicationMasterResponse register() throws YarnException, IOException {
		// register with RM and start heartbeat
		return client.registerApplicationMaster(NetUtils.getHostname(), amRpcPort, amTrackingUrl);
	}

	public void stop(FinalApplicationStatus finalStatus, String retMessage) {
		try {
			// signal RM for completion
			log.info("signalling RM to unregister and stop");
			client.unregisterApplicationMaster(finalStatus, retMessage, amTrackingUrl);
		} catch (YarnException | IOException e) {
			log.error("error in unregistering AM", e);
		}
		client.stop();
	}

	public void requestForContainer(ApplicationMasterProperties properties) {
		// setup priority
		Priority pri = Records.newRecord(Priority.class);
		pri.setPriority(properties.getPriority());

		// setup resource type requirements
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(properties.getContainerMemory());
		capability.setVirtualCores(properties.getContainerVirtualCores());

		// create container request
		ContainerRequest request = new ContainerRequest(capability, null, null, pri);
		client.addContainerRequest(request);
	}
}
