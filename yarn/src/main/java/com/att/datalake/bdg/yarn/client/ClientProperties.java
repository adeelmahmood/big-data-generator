package com.att.datalake.bdg.yarn.client;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "yarn.client")
public class ClientProperties {

	private String name;
	private int priority;
	private String queue;
	private int masterMemory;
	private int masterVirtualCores;
	private boolean keepContainerAcrossRestart;
	private boolean debug;

	public int getMasterMemory() {
		return masterMemory;
	}

	public void setMasterMemory(int masterMemory) {
		this.masterMemory = masterMemory;
	}

	public int getMasterVirtualCores() {
		return masterVirtualCores;
	}

	public void setMasterVirtualCores(int masterVirtualCores) {
		this.masterVirtualCores = masterVirtualCores;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public String getQueue() {
		return queue;
	}

	public void setQueue(String queue) {
		this.queue = queue;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean isKeepContainerAcrossRestart() {
		return keepContainerAcrossRestart;
	}

	public void setKeepContainerAcrossRestart(boolean keepContainerAcrossRestart) {
		this.keepContainerAcrossRestart = keepContainerAcrossRestart;
	}
}
