package com.att.datalake.bdg.yarn.am.events;

import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.springframework.context.ApplicationEvent;

public class ContainerFailedEvent extends ApplicationEvent {

	private static final long serialVersionUID = -2409263198879706140L;

	public ContainerFailedEvent(ContainerStatus status) {
		super(status);
	}

}
