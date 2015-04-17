package com.att.datalake.bdg.yarn.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.att.datalake.bdg.yarn.Application;
import com.att.datalake.bdg.yarn.support.ResourceHandler;
import com.att.datalake.bdg.yarn.utils.YarnUtils;

@Component
public class ApplicationClient extends AbstractYarnClient {

	private static final Logger log = LoggerFactory.getLogger(ApplicationClient.class);

	private final YarnClient yarnClient;
	private final ClientProperties clientProperties;
	private final YarnClientOperations clientOperations;
	private final ResourceHandler resourceHandler;

	@Value("${yarn.containerJar}")
	private String containerJar;

	@Autowired
	public ApplicationClient(ClientProperties clientProperties, YarnClient yarnClient,
			YarnClientOperations clientOperations, ResourceHandler resourceHandler) {
		this.clientProperties = clientProperties;
		this.yarnClient = yarnClient;
		this.clientOperations = clientOperations;
		this.resourceHandler = resourceHandler;
	}

	public void start() throws YarnException, IOException {
		log.info("yarn client starting");
		yarnClient.start();

		if (clientProperties.isDebug()) {
			YarnUtils.printClusterStats(yarnClient, clientProperties.getQueue());
		}

		// create new yarn application
		YarnClientApplication app = clientOperations.getNewApplication(clientProperties);

		// create application submission context
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		ApplicationId appId = appContext.getApplicationId();

		// set application general attributes
		appContext.setApplicationName(clientProperties.getName());
		appContext.setKeepContainersAcrossApplicationAttempts(clientProperties.isKeepContainerAcrossRestart());

		// create application master container request
		ContainerLaunchContext appContainer = createMasterLaunchRequest(appId);

		// set the command to start up the application master
		setMasterCommand(appContainer);

		// set application master requirements
		setMasterRequirements(appContext, appContainer);

		// submit application
		yarnClient.submitApplication(appContext);

		// continue to monitor the application
		monitor(appId, clientProperties.isDebug());
	}

	private ContainerLaunchContext createMasterLaunchRequest(ApplicationId appId) throws IOException {
		// create application master container
		ContainerLaunchContext appContainer = Records.newRecord(ContainerLaunchContext.class);

		// set local resource for application master
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

		// add application master jar to resources
		resourceHandler.addLocalResource(Application.CURRENT_JAR_PATH,
				FilenameUtils.getName(Application.CURRENT_JAR_PATH), clientProperties.getName(), appId.toString(),
				localResources);
		// add container jar to resources
		resourceHandler.addLocalResource(containerJar, FilenameUtils.getName(containerJar), clientProperties.getName(),
				appId.toString(), localResources);

		// specify local resource on container
		appContainer.setLocalResources(localResources);

		// create environment
		Map<String, String> env = getClasspath();
		if (clientProperties.isDebug()) {
			log.info("Classpath set as => " + env);
		}
		// specify environment on container
		appContainer.setEnvironment(env);

		return appContainer;
	}

	private void setMasterCommand(ContainerLaunchContext appContainer) {
		// create a the command to execute application master
		Vector<CharSequence> vargs = new Vector<CharSequence>();
		vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
		vargs.add("-Xmx" + clientProperties.getMasterMemory() + "m");
		// specify jar to run
		vargs.add("-jar " + FilenameUtils.getName(Application.CURRENT_JAR_PATH));
		// params
		vargs.add("application-master");
		// logs
		vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
		vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

		// set command on container to run application master
		appContainer.setCommands(YarnUtils.createCommand(vargs));
	}

	private void setMasterRequirements(ApplicationSubmissionContext appContext, ContainerLaunchContext appContainer)
			throws IOException {
		// specify container requirements
		Resource resource = Records.newRecord(Resource.class);
		resource.setMemory(clientProperties.getMasterMemory());
		resource.setVirtualCores(clientProperties.getMasterVirtualCores());
		appContext.setResource(resource);
		log.info("container capability set as " + clientProperties.getMasterMemory() + "m memory and "
				+ clientProperties.getMasterVirtualCores() + " virtual cores");

		// setup security tokens
		ByteBuffer tokens = clientOperations.obtainSecurityTokens();
		if (tokens != null) {
			appContainer.setTokens(tokens);
		}

		// container setup complete, set on context
		appContext.setAMContainerSpec(appContainer);

		// set priroity
		Priority pri = Records.newRecord(Priority.class);
		pri.setPriority(clientProperties.getPriority());
		appContext.setPriority(pri);
		log.info("using priority " + pri);

		// set queue
		appContext.setQueue(clientProperties.getQueue());
		log.info("using queue " + clientProperties.getQueue());
	}

	@Override
	YarnClient getYarnClient() {
		return yarnClient;
	}
}
