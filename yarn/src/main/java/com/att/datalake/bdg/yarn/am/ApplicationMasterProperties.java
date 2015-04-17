package com.att.datalake.bdg.yarn.am;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "yarn.am")
public class ApplicationMasterProperties {

	private String name;
	private int numContainers;
	private int containerMemory;
	private int containerVirtualCores;
	private int priority;
	private boolean debug;
	private String appAttemptId;

	public int getNumContainers() {
		return numContainers;
	}

	public void setNumContainers(int numContainers) {
		this.numContainers = numContainers;
	}

	public int getContainerMemory() {
		return containerMemory;
	}

	public void setContainerMemory(int containerMemory) {
		this.containerMemory = containerMemory;
	}

	public int getContainerVirtualCores() {
		return containerVirtualCores;
	}

	public void setContainerVirtualCores(int containerVirtualCores) {
		this.containerVirtualCores = containerVirtualCores;
	}

	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public String getAppAttemptId() {
		return appAttemptId;
	}

	public void setAppAttemptId(String appAttemptId) {
		this.appAttemptId = appAttemptId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
