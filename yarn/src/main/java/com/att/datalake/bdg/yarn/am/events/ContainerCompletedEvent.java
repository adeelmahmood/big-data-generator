package com.att.datalake.bdg.yarn.am.events;

import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.springframework.context.ApplicationEvent;

public class ContainerCompletedEvent extends ApplicationEvent {

	private static final long serialVersionUID = -2639209330536615188L;

	public ContainerCompletedEvent(ContainerStatus status) {
		super(status);
	}

}
