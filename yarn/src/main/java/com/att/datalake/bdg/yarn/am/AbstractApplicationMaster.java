package com.att.datalake.bdg.yarn.am;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

public abstract class AbstractApplicationMaster {

	private static final Logger log = LoggerFactory.getLogger(AbstractApplicationMaster.class);

	protected final AMRMClient amRMClient;
	protected final AMNMClient amNMClient;
	protected final FileSystem fs;

	protected int numContainersToRequest;

	protected AtomicInteger numRequestedContainers = new AtomicInteger();
	protected AtomicInteger numFailedContainers = new AtomicInteger();
	protected AtomicInteger numAllocatedContainers = new AtomicInteger();

	private boolean done;

	public AbstractApplicationMaster(AMRMClient amRMClient, AMNMClient amNMClient, FileSystem fs) {
		this.amRMClient = amRMClient;
		this.amNMClient = amNMClient;
		this.fs = fs;
	}

	protected ApplicationAttemptId checkForApplicationAttemptId(ApplicationMasterProperties properties) {
		Map<String, String> env = System.getenv();
		ApplicationAttemptId appAttemptId;

		// gather application attempt id (attempt id because of possible
		// failure for AM)
		if (!env.containsKey(Environment.CONTAINER_ID.name())) {
			// expect it to be passed in
			if (!StringUtils.isEmpty(properties.getAppAttemptId())) {
				appAttemptId = ConverterUtils.toApplicationAttemptId(properties.getAppAttemptId());
			} else {
				throw new IllegalArgumentException("app_attempt_id not set in environment");
			}
		} else {
			ContainerId containerId = ConverterUtils.toContainerId(env.get(Environment.CONTAINER_ID.name()));
			appAttemptId = containerId.getApplicationAttemptId();
		}
		return appAttemptId;
	}

	protected void finish() {
		log.info("signalling NM to stop all containers");
		// signal NM to stop all containers
		amNMClient.stop();

		// signal RM for completion
		log.info("signalling RM to unregister and stop");
		amRMClient.stop(FinalApplicationStatus.SUCCEEDED, "application master exiting");
	}

	public boolean isDone() {
		return done;
	}

	public void setDone(boolean done) {
		this.done = done;
	}
}
