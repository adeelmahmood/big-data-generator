package com.att.datalake.bdg.yarn.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractYarnClient {

	private static final Logger log = LoggerFactory.getLogger(AbstractYarnClient.class);

	protected Map<String, String> getClasspath() {
		// create environment
		Map<String, String> env = new HashMap<String, String>();

		// set classpath
		StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$()).append(
				ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
		for (String c : getYarnClient().getConfig().getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
			classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
			classPathEnv.append(c.trim());
		}
		env.put("CLASSPATH", classPathEnv.toString());
		return env;
	}

	protected boolean monitor(ApplicationId appId, boolean debug) throws YarnException, IOException {
		while (true) {
			try {
				// check status every second
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}

			// get application status report
			ApplicationReport report = getYarnClient().getApplicationReport(appId);
			if (debug) {
				log.info("Got application report from ASM for" + ", appId=" + appId.getId() + ", clientToAMToken="
						+ report.getClientToAMToken() + ", appDiagnostics=" + report.getDiagnostics()
						+ ", appMasterHost=" + report.getHost() + ", appQueue=" + report.getQueue()
						+ ", appMasterRpcPort=" + report.getRpcPort() + ", appStartTime=" + report.getStartTime()
						+ ", yarnAppState=" + report.getYarnApplicationState().toString() + ", distributedFinalState="
						+ report.getFinalApplicationStatus().toString() + ", appTrackingUrl=" + report.getTrackingUrl()
						+ ", appUser=" + report.getUser());
			}

			// get application statuses
			YarnApplicationState yarnStatus = report.getYarnApplicationState();
			FinalApplicationStatus finalStatus = report.getFinalApplicationStatus();
			if (FinalApplicationStatus.SUCCEEDED == finalStatus) {
				if (YarnApplicationState.FINISHED == yarnStatus) {
					log.info("application completed successfully, stopping monitoring");
					return true;
				} else {
					log.info("application did not completed successfully. yarn status = " + yarnStatus.toString()
							+ ", final status = " + finalStatus.toString());
					return false;
				}
			} else if (FinalApplicationStatus.KILLED == finalStatus || FinalApplicationStatus.FAILED == finalStatus) {
				log.info("application ended prematurely. yarn status = " + yarnStatus.toString() + ", final status = "
						+ finalStatus.toString());
				return false;
			}
		}
	}

	abstract YarnClient getYarnClient();
}
