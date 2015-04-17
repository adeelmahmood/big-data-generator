package com.att.datalake.bdg.yarn.am;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.att.datalake.bdg.yarn.utils.YarnUtils;

@Component
public class ApplicationMaster extends AbstractApplicationMaster {

	private static final Logger log = LoggerFactory.getLogger(ApplicationMaster.class);

	private final ApplicationMasterProperties amProperties;

	private ApplicationAttemptId appAttemptId;
	private ByteBuffer allTokens;
	private UserGroupInformation ugi;

	@Value("${yarn.AcontainerJar}")
	private String containerJar;

	@Autowired
	public ApplicationMaster(ApplicationMasterProperties amProperties, AMRMClient amRMClient, AMNMClient amNMClient,
			FileSystem fs) {
		super(amRMClient, amNMClient, fs);
		this.amProperties = amProperties;
	}

	public void start() throws IOException, YarnException {
		log.info("application master starting");
		Map<String, String> env = System.getenv();

		// set the app attempt id if specified
		appAttemptId = checkForApplicationAttemptId(amProperties);

		// make sure environment is setup correctly
		YarnUtils.checkApplicationEnvRequiredParams(env);

		if (amProperties.isDebug()) {
			log.info("Application master for app" + ", appId=" + appAttemptId.getApplicationId().getId()
					+ ", clustertimestamp=" + appAttemptId.getApplicationId().getClusterTimestamp() + ", attemptId="
					+ appAttemptId.getAttemptId());
			YarnUtils.printEnvAndClasspath();
		}

		// extract security tokens and prepare ugi
		extractSecurityTokens();

		// initiate communication with resource manager
		amRMClient.init();

		// initiate communication with node manager
		amNMClient.init();

		// register heartbeat with RM
		RegisterApplicationMasterResponse response = amRMClient.register();
		adjustContainerRequirements(response);

		// get previously running containers
		List<Container> previouslyRunningContainers = response.getContainersFromPreviousAttempts();
		if (amProperties.isDebug()) {
			log.info("found " + previouslyRunningContainers.size() + " previously running containers");
		}
		numAllocatedContainers.addAndGet(previouslyRunningContainers.size());

		// make container jar available for containers
		String suffix = amProperties.getName() + Path.SEPARATOR + appAttemptId.getApplicationId() + Path.SEPARATOR
				+ FilenameUtils.getName(containerJar);
		Path dest = new Path(fs.getHomeDirectory(), suffix);
		fs.copyFromLocalFile(new Path(FilenameUtils.getName(containerJar)), dest);

		// request containers
		numContainersToRequest = amProperties.getNumContainers() - previouslyRunningContainers.size();
		requestForContainers();
	}

	private void requestForContainers() {
		int totalContainersToRequest = numContainersToRequest - numRequestedContainers.get();
		if (totalContainersToRequest > 0) {
			log.info("requesting " + totalContainersToRequest + " new containers");
			for (int i = 0; i < totalContainersToRequest; ++i) {
				amRMClient.requestForContainer(amProperties);
			}
		}
		numRequestedContainers.addAndGet(totalContainersToRequest);
	}

	private void extractSecurityTokens() throws IOException {
		// gather submitted credentials
		Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
		DataOutputBuffer dob = new DataOutputBuffer();
		credentials.writeTokenStorageToStream(dob);
		// Now remove the AM->RM token so that containers cannot access it.
		Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
		log.debug("Executing with tokens:");
		while (iter.hasNext()) {
			Token<?> token = iter.next();
			log.debug(token.toString());
			if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
				iter.remove();
			}
		}
		allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

		// Create appSubmitterUgi and add original tokens to it
		String appSubmitterUserName = System.getenv(ApplicationConstants.Environment.USER.name());
		ugi = UserGroupInformation.createRemoteUser(appSubmitterUserName);
		ugi.addCredentials(credentials);
	}

	private void adjustContainerRequirements(RegisterApplicationMasterResponse response) {
		// get container capabilities and adjust our requirements
		int maxMem = response.getMaximumResourceCapability().getMemory();
		log.info("maximum memory allowed => " + maxMem);
		if (amProperties.getContainerMemory() > maxMem) {
			log.info("request container memory " + amProperties.getContainerMemory() + " is more than max memory "
					+ maxMem + ", adjusting");
			amProperties.setContainerMemory(maxMem);
		}

		int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
		log.info("maximum virtual cores allowed => " + maxVCores);
		if (amProperties.getContainerVirtualCores() > maxVCores) {
			log.info("requested virtual cores " + amProperties.getContainerVirtualCores()
					+ " is more than max virtual cores " + maxVCores + ", adjusting");
			amProperties.setContainerVirtualCores(maxVCores);
		}
	}
}
