package com.att.datalake.bdg.yarn.client;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class YarnClientOperations implements ClientOperations {

	private static final Logger log = LoggerFactory.getLogger(YarnClientOperations.class);

	private final YarnClient yarnClient;
	private final FileSystem fs;

	@Autowired
	public YarnClientOperations(YarnClient yarnClient, FileSystem fs) {
		this.yarnClient = yarnClient;
		this.fs = fs;
	}

	@Override
	public YarnClientApplication getNewApplication(ClientProperties properties) {
		try {
			YarnClientApplication app = yarnClient.createApplication();
			GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

			// get container capabilities and adjust our requirements
			int maxMem = appResponse.getMaximumResourceCapability().getMemory();
			log.info("maximum memory allowed => " + maxMem);
			if (properties.getMasterMemory() > maxMem) {
				log.info("request application master memory " + properties.getMasterMemory()
						+ " is more than max memory " + maxMem + ", adjusting");
				properties.setMasterMemory(maxMem);
			}

			int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
			log.info("maximum virtual cores allowed => " + maxVCores);
			if (properties.getMasterVirtualCores() > maxVCores) {
				log.info("requested virtual cores " + properties.getMasterVirtualCores()
						+ " is more than max virtual cores " + maxVCores + ", adjusting");
				properties.setMasterVirtualCores(maxVCores);
			}
			return app;
		} catch (YarnException | IOException e) {
			log.error("error in creating new application", e);
		}
		return null;
	}

	@Override
	public ByteBuffer obtainSecurityTokens() throws IOException {
		// Setup security tokens
		if (UserGroupInformation.isSecurityEnabled()) {
			Credentials credentials = new Credentials();
			String tokenRenewer = yarnClient.getConfig().get(YarnConfiguration.RM_PRINCIPAL);
			if (tokenRenewer == null || tokenRenewer.length() == 0) {
				throw new IOException("Can't get Master Kerberos principal for the RM to use as renewer");
			}

			// For now, only getting tokens for the default file-system.
			final Token<?> tokens[] = fs.addDelegationTokens(tokenRenewer, credentials);
			if (tokens != null) {
				for (Token<?> token : tokens) {
					log.info("Got dt for " + fs.getUri() + "; " + token);
				}
			}
			DataOutputBuffer dob = new DataOutputBuffer();
			credentials.writeTokenStorageToStream(dob);
			ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
			return fsTokens;
		}
		return null;
	}
}
